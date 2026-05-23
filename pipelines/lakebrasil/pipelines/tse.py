"""TSE eleições históricas (consulta_cand) → data_br.eleicoes_municipais.

Migra `_loaders/load_tse.py` (SQLite) para o padrão dlt + Iceberg:
- raw em S3 (`tse/raw/consulta_cand_{ano}.zip`)
- TSE→IBGE via `data_br.municipios.codigo_tse`
- Filtra cargos municipais (PREFEITO, VICE-PREFEITO, VEREADOR) — anos
  estaduais/federais ainda têm registros mas com ibge_code=null então
  pulamos pra manter a tabela coerente com `eleicoes_municipais`.
- Idempotente: lê os anos já carregados em Iceberg e pula.

CSV interno latin-1, separador ;, nome = `consulta_cand_{ano}_BRASIL.csv`.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.tse --year 2024 --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.tse                  # todos os anos não-carregados
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.tse --full-refresh   # reprocessa tudo
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
from typing import Iterator

import dlt

from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.s3 import list_keys, stream_zip_csv
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "tse/raw/"
FILE_RE = re.compile(r"^consulta_cand_(\d{4})\.zip$")
VOTACAO_FILE_RE = re.compile(r"^votacao_candidato_munzona_(\d{4})\.zip$")
CARGOS = {"PREFEITO", "VICE-PREFEITO", "VEREADOR"}


def _build_tse_to_ibge() -> dict[str, int]:
    """codigo_tse na dim `data_br.municipios` → ibge_code.

    O TSE usa SG_UE de 5 dígitos sem zero à esquerda — normalizamos pra
    bater com o codigo_tse que veio do seed `municipios-br.sqlite`."""
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "codigo_tse")
    ).to_arrow()
    out: dict[str, int] = {}
    for ibge, tse in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("codigo_tse").to_pylist(),
    ):
        if tse is None:
            continue
        key = str(tse).strip().lstrip("0") or "0"
        out[key] = int(ibge)
    return out


def _years_already_loaded() -> set[int]:
    """Anos já materializados em `data_br.eleicoes_municipais`. Pipeline
    pula esses → idempotência. Empty se a tabela não existe ainda."""
    from lakebrasil.loaders.iceberg import catalog
    from pyiceberg.exceptions import NoSuchTableError
    try:
        t = catalog().load_table("data_br.eleicoes_municipais")
    except NoSuchTableError:
        return set()
    if not t.metadata.snapshots:
        return set()
    arrow = t.scan(selected_fields=("ano",)).to_arrow()
    return {int(a) for a in arrow.column("ano").to_pylist() if a is not None}


def _parse_int(s: str | None, default: int = 0) -> int:
    if not s:
        return default
    try:
        return int(s.strip())
    except ValueError:
        return default


def _build_votes_idx(
    s3_key: str,
    ano: int,
    idx_tse: dict[str, int],
) -> dict[tuple[int, int, str], int]:
    """Para um votacao_candidato_munzona_{ano}.zip, agrega votos por
    (turno, ibge_code, sq_candidato). Múltiplas zonas no mesmo município
    são somadas — granularidade alvo da `eleicoes_municipais` é por-município.

    Streama o BRASIL.csv consolidado (latin-1, ;-separated). Memória
    pico ~50 MB pra 2024 (350 MB CSV); o dict resultante tem ~1.5M
    entradas (~200 MB worst-case).

    Retorna {} se o arquivo não existe (anos < 2008 podem não ter dump).
    """
    target_csv = f"votacao_candidato_munzona_{ano}_BRASIL.csv"
    out: dict[tuple[int, int, str], int] = {}
    try:
        for inner, fh in stream_zip_csv(
            s3_key, csv_filter=lambda n: n == target_csv,
        ):
            text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="")
            reader = csv.DictReader(text_io, delimiter=";")
            for r in reader:
                try:
                    turno = _parse_int(r.get("NR_TURNO"), 1)
                    sg_ue = str(r.get("SG_UE", "")).strip().lstrip("0") or "0"
                    ibge = idx_tse.get(sg_ue)
                    if ibge is None:
                        continue
                    sq = str(r.get("SQ_CANDIDATO", "")).strip()
                    if not sq:
                        continue
                    votos = _parse_int(r.get("QT_VOTOS_NOMINAIS"), 0)
                    out[(turno, ibge, sq)] = out.get((turno, ibge, sq), 0) + votos
                except (ValueError, KeyError):
                    continue
    except FileNotFoundError:
        # Anos antigos podem não ter votacao_candidato_munzona — ok.
        pass
    return out


def _iter_year(
    s3_key: str,
    ano: int,
    idx_tse: dict[str, int],
    votes_idx: dict[tuple[int, int, str], int] | None = None,
) -> Iterator[dict]:
    target_csv = f"consulta_cand_{ano}_BRASIL.csv"
    misses = 0
    emitted = 0
    for inner, fh in stream_zip_csv(
        s3_key, csv_filter=lambda n: n == target_csv,
    ):
        text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="")
        reader = csv.DictReader(text_io, delimiter=";")
        for r in reader:
            cargo = r.get("DS_CARGO", "").strip().upper()
            if cargo not in CARGOS:
                continue
            sg_ue = str(r.get("SG_UE", "")).strip().lstrip("0") or "0"
            ibge = idx_tse.get(sg_ue)
            if ibge is None:
                misses += 1
                continue
            try:
                yield {
                    "ano":          int(r["ANO_ELEICAO"]),
                    "turno":        _parse_int(r.get("NR_TURNO"), 1),
                    "ibge_code":    ibge,
                    "uf":           (r.get("SG_UF") or r.get("SG_UE_SUPERIOR") or "").strip().upper() or None,
                    "cargo":        cargo,
                    "sq_candidato": str(r["SQ_CANDIDATO"]).strip(),
                    "nome":         (r.get("NM_CANDIDATO") or "").strip() or None,
                    "nome_urna":    (r.get("NM_URNA_CANDIDATO") or "").strip() or None,
                    "partido":      (r.get("SG_PARTIDO") or "").strip() or None,
                    "coligacao":    (r.get("NM_COLIGACAO") or "").strip() or None,
                    "situacao":     (r.get("DS_SIT_TOT_TURNO") or "").strip() or None,
                    "votos":        (votes_idx.get((_parse_int(r.get("NR_TURNO"), 1), ibge, str(r["SQ_CANDIDATO"]).strip()))
                                     if votes_idx else None),
                    "genero":       (r.get("DS_GENERO") or "").strip() or None,
                    "cor_raca":     (r.get("DS_COR_RACA") or "").strip() or None,
                    "escolaridade": (r.get("DS_GRAU_INSTRUCAO") or "").strip() or None,
                    "ocupacao":     (r.get("DS_OCUPACAO") or "").strip() or None,
                    "nasc":         (r.get("DT_NASCIMENTO") or "").strip() or None,
                    "cpf":          (r.get("NR_CPF_CANDIDATO") or "").strip() or None,
                }
                emitted += 1
            except (ValueError, KeyError):
                continue
    print(f"  TSE {ano}: emitted={emitted} misses={misses}", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--year", type=int, help="YYYY single year (debug).")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-fetch", action="store_true",
                   help="Pula download (usa raw em S3 como está).")
    p.add_argument("--refresh", action="store_true",
                   help="Força re-download mesmo com manifest sha256 batendo.")
    p.add_argument("--full-refresh", action="store_true",
                   help="Reprocessa TODOS os anos mesmo já carregados — drop "
                        "table antes pra reload limpo, senão duplica.")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        # tse_consulta_cand expandido por ano em catalog.yaml.
        ensure_fetched("tse_consulta_cand", refresh=args.refresh)

    idx_tse = _build_tse_to_ibge()
    print(f"TSE→IBGE índice: {len(idx_tse)} municípios")
    already = set() if (args.full_refresh or args.year) else _years_already_loaded()
    if already:
        print(f"  já carregados: {sorted(already)}")

    @dlt.resource(
        # write_disposition="merge" exigiria primary_key=(ano, turno, ibge,
        # cargo, sq_candidato) — em vez disso filtramos anos já carregados
        # no resource e usamos append (mesmo padrão de beneficios.py).
        name="eleicoes_municipais",
        write_disposition="append",
    )
    def eleicoes() -> Iterator[dict]:
        # Pré-mapeia consulta_cand e votacao_candidato_munzona por ano.
        # Iteramos consulta_cand mas precisamos do votacao do MESMO ano
        # antes (build votes_idx → enriquece os yields).
        cand_keys: dict[int, str] = {}
        votacao_keys: dict[int, str] = {}
        for key in sorted(list_keys(S3_PREFIX)):
            name = key.rsplit("/", 1)[-1]
            m = FILE_RE.match(name)
            if m:
                cand_keys[int(m.group(1))] = key
                continue
            mv = VOTACAO_FILE_RE.match(name)
            if mv:
                votacao_keys[int(mv.group(1))] = key

        for ano, key in sorted(cand_keys.items()):
            if args.year and ano != args.year:
                continue
            if ano in already:
                continue
            t0 = time.monotonic()
            votes_idx: dict[tuple[int, int, str], int] | None = None
            if ano in votacao_keys:
                t_v = time.monotonic()
                votes_idx = _build_votes_idx(votacao_keys[ano], ano, idx_tse)
                print(f"  TSE {ano}: votos idx={len(votes_idx):,} entries em "
                      f"{time.monotonic()-t_v:.1f}s", file=sys.stderr)
            else:
                print(f"  TSE {ano}: sem votacao_candidato_munzona — votos=NULL",
                      file=sys.stderr)
            yield from _iter_year(key, ano, idx_tse, votes_idx)
            print(f"  TSE {ano}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="tse_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(eleicoes())
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT ano, cargo, count(*) FROM eleicoes_municipais "
                                "GROUP BY ano, cargo ORDER BY ano, cargo"))
        return 0

    pipe = dlt.pipeline(pipeline_name="tse", destination=s3tables_iceberg)
    info = pipe.run(eleicoes())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
