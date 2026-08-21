"""Câmara dos Deputados (proposições + votações + votos) → Iceberg.

3 resources / 3 tabelas materializadas em S3 Tables Iceberg:
- `data_br.camara_proposicoes` — uma linha por proposição (PL/PEC/MPV/...)
- `data_br.camara_votacoes`    — uma linha por evento de votação
- `data_br.camara_votos`       — voto individual de cada deputado

Raw em S3 (`camara/raw/proposicoes-{ano}.json` etc.) baixado via
`lakebrasil.scripts.fetch` com fetcher `http` (catalog.yaml: camara_proposicoes,
camara_votacoes, camara_votos). Anos 2015 → presente.

Idempotência: cada tabela rastreia anos já carregados em Iceberg e
pula. `--full-refresh` reprocessa tudo (DROP TABLE antes pra evitar
duplicação).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.camara --year 2024 --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.camara --resource proposicoes
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.camara                  # tudo
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections.abc import Iterator

import dlt

from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "camara/raw/"
PROPOSICOES_RE   = re.compile(r"^proposicoes-(\d{4})\.json$")
VOTACOES_RE      = re.compile(r"^votacoes-(\d{4})\.json$")
VOTOS_RE         = re.compile(r"^votacoesVotos-(\d{4})\.json$")

RESOURCES = {
    "proposicoes": ("camara_proposicoes", PROPOSICOES_RE, "ano"),
    "votacoes":    ("camara_votacoes",    VOTACOES_RE,    "ano"),
    "votos":       ("camara_votos",       VOTOS_RE,       "ano"),
}


def _years_already_loaded(table: str) -> set[int]:
    """Anos já em Iceberg pra essa tabela. Empty se não existe ainda."""
    from pyiceberg.exceptions import NoSuchTableError

    from lakebrasil.loaders.iceberg import catalog
    try:
        t = catalog().load_table(f"data_br.{table}")
    except NoSuchTableError:
        return set()
    if not t.metadata.snapshots:
        return set()
    arrow = t.scan(selected_fields=("ano",)).to_arrow()
    return {int(a) for a in arrow.column("ano").to_pylist() if a is not None}


def _maybe_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _read_json_array(s3_key: str) -> list[dict]:
    """Câmara empacota arquivos como `{"dados": [...]}` (shape do export
    da API REST). Aceita também array puro pra resiliência. Até ~200 MB
    cabe na memória; acima disso, considere ijson streaming."""
    body = get_object_bytes(s3_key)
    payload = json.loads(body)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("dados") or []
    return []


# ── proposicoes ────────────────────────────────────────────────────────────

def _iter_proposicoes(s3_key: str, ano: int) -> Iterator[dict]:
    for r in _read_json_array(s3_key):
        # `siglaOrgao` / `uriOrgao` aparecem dentro de `ultimoStatus` no
        # export da Câmara — não no nível root.
        ultimo = r.get("ultimoStatus") or {}
        yield {
            "id":                _maybe_int(r.get("id")),
            "ano":               ano,
            "sigla_tipo":        r.get("siglaTipo"),
            "numero":            _maybe_int(r.get("numero")),
            "ementa":            r.get("ementa"),
            "ementa_detalhada":  r.get("ementaDetalhada"),
            "keywords":          r.get("keywords"),
            "data_apresentacao": r.get("dataApresentacao"),
            "sigla_orgao":       ultimo.get("siglaOrgao"),
            "uri_orgao":         ultimo.get("uriOrgao"),
            "uri_proposicao":    r.get("uri"),
        }


# ── votacoes ───────────────────────────────────────────────────────────────

def _iter_votacoes(s3_key: str, ano: int) -> Iterator[dict]:
    for r in _read_json_array(s3_key):
        yield {
            "id":                  r.get("id"),
            "ano":                 ano,
            "data":                r.get("data"),
            "data_hora_registro":  r.get("dataHoraRegistro"),
            "sigla_orgao":         r.get("siglaOrgao"),
            "descricao":           r.get("descricao"),
            "aprovacao":           _maybe_int(r.get("aprovacao")),
            "ultima_apresentacao_proposicao": (
                (r.get("ultimaApresentacaoProposicao") or {}).get("descricao")
            ),
            "uri":                 r.get("uri"),
        }


# ── votos ──────────────────────────────────────────────────────────────────

def _iter_votos(s3_key: str, ano: int) -> Iterator[dict]:
    for r in _read_json_array(s3_key):
        dep = r.get("deputado_") or {}
        yield {
            "id_votacao":   r.get("idVotacao"),
            "id_deputado":  _maybe_int(dep.get("id")),
            "ano":          ano,
            "voto":         r.get("voto") or r.get("tipoVoto"),
            "data_hora":    r.get("dataRegistroVoto") or r.get("dataHoraVoto"),
            "sigla_partido": dep.get("siglaPartido"),
            "sigla_uf":     dep.get("siglaUf"),
        }


ITER_BY_RESOURCE = {
    "proposicoes": _iter_proposicoes,
    "votacoes":    _iter_votacoes,
    "votos":       _iter_votos,
}


def _build_resource(name: str, only_year: int | None,
                    full_refresh: bool) -> Iterator[dict]:
    table, pattern, _ = RESOURCES[name]
    iterator = ITER_BY_RESOURCE[name]
    already = set() if (full_refresh or only_year) else _years_already_loaded(table)
    if already:
        print(f"  {name}: anos já em Iceberg → {sorted(already)}", file=sys.stderr)

    for key in sorted(list_keys(S3_PREFIX)):
        fname = key.rsplit("/", 1)[-1]
        m = pattern.match(fname)
        if not m:
            continue
        ano = int(m.group(1))
        if only_year and ano != only_year:
            continue
        if ano in already:
            continue
        t0 = time.monotonic()
        n = 0
        for rec in iterator(key, ano):
            n += 1
            yield rec
        print(f"  {name} {ano}: {n:,} linhas em {time.monotonic()-t0:.1f}s",
              file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--resource", choices=tuple(RESOURCES),
                   help="Default: todos (proposicoes + votacoes + votos).")
    p.add_argument("--year", type=int, help="YYYY single year (debug).")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-fetch", action="store_true",
                   help="Pula download (usa raw em S3 como está).")
    p.add_argument("--refresh", action="store_true",
                   help="Força re-download mesmo com manifest sha256 batendo.")
    p.add_argument("--full-refresh", action="store_true",
                   help="Reprocessa TODOS os anos mesmo já carregados.")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    selected = [args.resource] if args.resource else list(RESOURCES)

    if not args.no_fetch:
        # cat names em catalog.yaml: camara_proposicoes, camara_votacoes, camara_votos.
        cat_names = [f"camara_{r}" for r in selected]
        ensure_fetched(cat_names, refresh=args.refresh)

    if args.dry_run:
        for resource_name in selected:
            table, _, _ = RESOURCES[resource_name]
            pipe = dlt.pipeline(
                pipeline_name=f"camara_{resource_name}_dryrun",
                destination="duckdb", dataset_name="staging", dev_mode=True,
            )

            @dlt.resource(name=table, write_disposition="append")
            def res(rn=resource_name):
                yield from _build_resource(rn, args.year, args.full_refresh)

            pipe.run(res())
            with pipe.sql_client() as c:
                print(c.execute_sql(f"SELECT ano, count(*) FROM {table} GROUP BY ano"))
        return 0

    for resource_name in selected:
        table, _, _ = RESOURCES[resource_name]

        @dlt.resource(name=table, write_disposition="append")
        def res(rn=resource_name):
            yield from _build_resource(rn, args.year, args.full_refresh)

        pipe = dlt.pipeline(pipeline_name=f"camara_{resource_name}",
                            destination=s3tables_iceberg)
        info = pipe.run(res())
        print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
