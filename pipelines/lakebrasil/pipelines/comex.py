"""Comex Stat — exportações + importações por município → data_br.comex_municipio.

Carrega 12 CSVs em `s3://data-br-raw/comex/raw/{EXP|IMP}_{ano}_MUN.csv`
(2020-2025) pra `data_br.comex_municipio`. Formato origem:

    CO_ANO;CO_MES;SH4;CO_PAIS;SG_UF_MUN;CO_MUN;KG_LIQUIDO;VL_FOB

Mapeamento → COMEX_SCHEMA:
  ano          ← CO_ANO
  mes          ← CO_MES
  ibge_code    ← CO_MUN (já é IBGE 7-dígitos)
  uf           ← SG_UF_MUN
  fluxo        ← 'EXP'|'IMP' do prefixo do filename
  sh4          ← SH4 (Sistema Harmonizado 4 dígitos — categoria produto)
  pais         ← CO_PAIS (código pais ISO numérico)
  kg_liquido   ← KG_LIQUIDO
  valor_fob    ← VL_FOB (R$)

Source: comexstat.mdic.gov.br — base anual atualizada pelo MDIC.
Volume: ~16M rows total (6 anos × 2 fluxos × ~5500 munis × ~30 SH4
× ~10 países em média).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.comex --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.comex --year 2024 --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "comex/raw/"
FILE_RE = re.compile(r"^(EXP|IMP)_(\d{4})_MUN\.csv$")


def _parse_int_safe(s: str | None) -> int | None:
    if not s or not s.strip():
        return None
    try:
        return int(s.strip())
    except ValueError:
        return None


def _iter_csv(s3_key: str, fluxo: str) -> Iterator[dict]:
    raw = get_object_bytes(s3_key)
    text_io = io.TextIOWrapper(io.BytesIO(raw), encoding="latin-1", newline="")
    reader = csv.DictReader(text_io, delimiter=";")
    rows_emitted = 0
    rows_skipped = 0
    for r in reader:
        try:
            ibge = _parse_int_safe(r.get("CO_MUN"))
            ano = _parse_int_safe(r.get("CO_ANO"))
            mes = _parse_int_safe(r.get("CO_MES"))
        except KeyError:
            rows_skipped += 1
            continue
        if not (ibge and ano and mes):
            rows_skipped += 1
            continue
        uf = (r.get("SG_UF_MUN") or "").strip().upper()
        sh4 = (r.get("SH4") or "").strip()
        pais = (r.get("CO_PAIS") or "").strip()
        if not (uf and sh4 and pais):
            rows_skipped += 1
            continue
        yield {
            "ano":         ano,
            "mes":         mes,
            "ibge_code":   ibge,
            "uf":          uf,
            "fluxo":       fluxo,
            "sh4":         sh4,
            "pais":        pais,
            "kg_liquido":  _parse_int_safe(r.get("KG_LIQUIDO")),
            "valor_fob":   _parse_int_safe(r.get("VL_FOB")),
        }
        rows_emitted += 1
    print(f"  COMEX {s3_key.rsplit('/',1)[-1]}: emitted={rows_emitted:,} skipped={rows_skipped:,}",
          file=sys.stderr)


def _years_loaded() -> set[tuple[str, int]]:
    """Set de (fluxo, ano) já em data_br.comex_municipio — pula re-load
    em dev. Empty se table vazia."""
    from pyiceberg.exceptions import NoSuchTableError

    from lakebrasil.loaders.iceberg import catalog
    try:
        t = catalog().load_table("data_br.comex_municipio")
    except NoSuchTableError:
        return set()
    if not t.metadata.snapshots:
        return set()
    arrow = t.scan(selected_fields=("fluxo", "ano")).to_arrow()
    return set(zip(arrow.column("fluxo").to_pylist(),
                   arrow.column("ano").to_pylist()))


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--year", type=int, help="YYYY single year (debug).")
    p.add_argument("--fluxo", choices=("EXP", "IMP"), help="Só um fluxo.")
    add_common_args(p, table_default="comex_municipio")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("comex_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/comex/raw/ já populado.",
                  file=sys.stderr)

    already = set() if args.full_refresh else _years_loaded()
    if already:
        print(f"  já carregados: {sorted(already)}", file=sys.stderr)

    @dlt.resource(
        name="comex_municipio",
        primary_key=["ano", "mes", "ibge_code", "fluxo", "sh4", "pais"],
        write_disposition="append",
    )
    def comex() -> Iterator[dict]:
        for key in sorted(list_keys(S3_PREFIX)):
            name = key.rsplit("/", 1)[-1]
            m = FILE_RE.match(name)
            if not m:
                continue
            fluxo, ano = m.group(1), int(m.group(2))
            if args.year and ano != args.year:
                continue
            if args.fluxo and fluxo != args.fluxo:
                continue
            if (fluxo, ano) in already:
                continue
            t0 = time.monotonic()
            yield from _iter_csv(key, fluxo)
            print(f"  COMEX {fluxo} {ano}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="comex_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(comex())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT fluxo, ano, count(*), sum(valor_fob) "
                "FROM comex_municipio GROUP BY fluxo, ano ORDER BY ano, fluxo"))
        return 0

    pipe = dlt.pipeline(pipeline_name="comex", destination=s3tables_iceberg)
    info = pipe.run(comex())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
