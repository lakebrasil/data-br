"""IBGE SIDRA → indicadores_serie.

SIDRA é a API de séries do IBGE. Um endpoint devolve sempre estrutura
JSON com `resultados[].series[].serie` indexada por período.

Cobre 3 fontes hoje (todas têm exato mesmo schema de resposta):
  - pam   (Produção Agrícola Municipal)
  - ppm   (Pesquisa da Pecuária Municipal)
  - registro_civil (RC nasc/casamentos por município)

Cada arquivo `{prefix}_{ano}.json` em S3 raw é um array de objetos:

    [{
      "id": "214",
      "variavel": "Quantidade produzida",
      "unidade": "Toneladas",
      "resultados": [{
        "classificacoes": [{"id": "81", "nome": "Produto das lavouras temp",
                            "categoria": {"0": "Total"}}],
        "series": [{
          "localidade": {"id": "1100015", ..., "nome": "Alta Floresta D'Oeste"},
          "serie": {"2023": ".."}                     # ".." = sem dado
        }, ...]
      }]
    }]

Mapeamento → `data_br.indicadores_serie`:
  ibge_code     ← localidade.id
  fonte         ← prefix do arquivo (pam/ppm/rc_nasc/...)
  indicador_id  ← `{fonte}.{variavel_id}.{classif_categoria_slug}`
  periodo       ← chave do dict serie ('2023')
  valor         ← float(serie value), filtro '..'
  unidade       ← unidade
  fonte_arquivo ← s3_key

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.common.sidra
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.common.sidra --source pam --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_pairs
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

# fonte_canonica → (s3_prefix, file_re_with_year_capture)
SOURCES = {
    "pam":            ("pam/raw/",            re.compile(r"^pam_temp_(\d{4})\.json$")),
    "ppm":            ("ppm/raw/",            re.compile(r"^ppm_(\d{4})\.json$")),
    "registro_civil": ("registro_civil/raw/", re.compile(r"^rc_nasc_(\d{4})\.json$")),
}


def _slug(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", s.lower().strip())
    return s.strip("_")[:40]


def _iter_records(fonte: str, key: str, ano: str) -> Iterator[dict]:
    """Parse 1 JSON SIDRA → records {ibge_code, fonte, indicador_id, periodo, valor}."""
    payload = json.loads(get_object_bytes(key))
    for variavel in payload:
        var_id = variavel.get("id", "?")
        var_name = variavel.get("variavel") or ""
        unidade = variavel.get("unidade")
        for resultado in variavel.get("resultados", []):
            # Classificação compõe o id final pra distinguir agregações.
            classif_parts = []
            for c in resultado.get("classificacoes", []):
                cat = c.get("categoria", {})
                # Pega o primeiro valor (Total ou label específico).
                val = next(iter(cat.values()), "")
                if val:
                    classif_parts.append(_slug(val))
            classif_slug = "_".join(classif_parts) if classif_parts else "total"
            indicador_id = f"{fonte}.{var_id}.{classif_slug}"
            for s in resultado.get("series", []):
                loc = s.get("localidade", {})
                ibge = loc.get("id")
                if not ibge:
                    continue
                try:
                    ibge_int = int(ibge)
                except (ValueError, TypeError):
                    continue
                # Filtra só municípios (N6) — algumas respostas trazem
                # estado (N3) ou país (N1) também.
                if loc.get("nivel", {}).get("id") != "N6":
                    continue
                serie = s.get("serie", {})
                for periodo, raw_val in serie.items():
                    if raw_val in ("..", "...", "-", None, ""):
                        continue
                    try:
                        valor = float(raw_val)
                    except (ValueError, TypeError):
                        continue
                    yield {
                        "ibge_code":     ibge_int,
                        "uf":            loc.get("nome", "").rsplit("-", 1)[-1].strip()[:2],
                        "fonte":         fonte,
                        "indicador_id":  indicador_id,
                        "periodo":       str(periodo),
                        "valor":         valor,
                        "valor_texto":   None,
                        "unidade":       unidade,
                        "fonte_arquivo": key.rsplit("/", 1)[-1],
                    }


@dlt.resource(
    name="indicadores_serie",
    primary_key=["ibge_code", "indicador_id", "periodo"],
    write_disposition="append",
    columns={
        "ibge_code":     {"data_type": "bigint", "nullable": False},
        "uf":            {"data_type": "text",   "nullable": True},
        "fonte":         {"data_type": "text",   "nullable": False},
        "indicador_id":  {"data_type": "text",   "nullable": False},
        "periodo":       {"data_type": "text",   "nullable": False},
        "valor":         {"data_type": "double", "nullable": True},
        "valor_texto":   {"data_type": "text",   "nullable": True},
        "unidade":       {"data_type": "text",   "nullable": True},
        "fonte_arquivo": {"data_type": "text",   "nullable": True},
    },
)
def sidra(only_source: str | None = None) -> Iterator[dict]:
    """Itera todas as fontes SIDRA configuradas, pulando o que já está
    em data_br.indicadores_serie via (fonte, periodo) + indicador_id."""
    # Já carregados: (indicador_id, periodo) tuples — granularidade
    # do dedup do SIDRA. Vai rejeitar re-load idempotente sem penalty.
    already = loaded_pairs("indicadores_serie", "fonte", "periodo")
    for fonte, (prefix, file_re) in SOURCES.items():
        if only_source and fonte != only_source:
            continue
        for key in sorted(list_keys(prefix)):
            name = key.rsplit("/", 1)[-1]
            m = file_re.match(name)
            if not m:
                continue
            ano = m.group(1)
            if (fonte, ano) in already:
                print(f"  sidra {fonte} {ano}: já carregado — skip", file=sys.stderr)
                continue
            n = 0
            for rec in _iter_records(fonte, key, ano):
                n += 1
                yield rec
            print(f"  sidra {fonte} {ano}: {n:,} registros", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--source", choices=tuple(SOURCES),
                   help="Default: todas (pam, ppm, registro_civil).")
    add_common_args(p, include_table=False)
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        # Catalog não tem entries pra esses ainda — cada um é fetcho
        # ad-hoc da API SIDRA. Pula fetch silencioso quando não há
        # match no catálogo.
        try:
            ensure_fetched("pam_*", refresh=args.refresh)
        except ValueError:
            pass

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="sidra_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(sidra(only_source=args.source))
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT fonte, count(*), count(DISTINCT ibge_code), "
                                "count(DISTINCT indicador_id) FROM indicadores_serie "
                                "GROUP BY fonte"))
        return 0

    pipe = dlt.pipeline(pipeline_name="sidra", destination=s3tables_iceberg)
    info = pipe.run(sidra(only_source=args.source))
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
