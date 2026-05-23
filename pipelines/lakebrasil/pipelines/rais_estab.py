"""RAIS ESTAB — Estabelecimentos formais por município → indicadores_serie.

Lê microdados RAIS Estabelecimentos do PDET/MTE (FTP) em:
  `s3://data-br-raw/rais/raw/RAIS_ESTAB_{ano}.7z`

1 row/estabelecimento. Schema (.COMT CSV header quoted, dados comma-sep):
  col 6  Qtd Vínculos CLT
  col 7  Qtd Vínculos Ativos
  col 8  Qtd Vínculos Estatutários
  col 12 Ind RAIS Negativa (1 = estab sem vínculos)
  col 14 Município - Código (IBGE 6-dig truncado, igual SIOPE/SNIS)
  col 18 Tamanho Estabelecimento - Código (1=0-4, 2=5-9, ..., 9=1000+)
  col 20 UF - Código

Agrega por (município, ano) em 8 indicadores:
  rais.estabelecimentos_total            count(*)
  rais.estabelecimentos_ativos           count where Vinc Ativos > 0
  rais.vinculos_ativos                   sum(Vinc Ativos)
  rais.vinculos_clt                      sum(Vinc CLT)
  rais.estabelecimentos_micro            tamanho 1-2 (0-9 empreg)
  rais.estabelecimentos_pequeno          tamanho 3-4 (10-49)
  rais.estabelecimentos_medio            tamanho 5-6 (50-249)
  rais.estabelecimentos_grande           tamanho 7-9 (250+)

Cobertura: 2022-2025 (anos não cobertos pelo CEMPRE/SIDRA que para
em 2021). ~5M estabs/ano × 4 anos.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.rais_estab --no-fetch
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import sys
import time
import tempfile
from collections import defaultdict
from typing import Iterator

import dlt
import py7zr

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "rais/raw/"
FILE_RE = re.compile(r"^RAIS_ESTAB_(\d{4})\.7z$")


def _build_co6_to_ibge_uf() -> tuple[dict[int, int], dict[int, str]]:
    """6-dig IBGE truncado → (7-dig, UF)."""
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    co6_to_ibge: dict[int, int] = {}
    ibge_to_uf: dict[int, str] = {}
    for ibge, uf in zip(arrow.column("ibge_code").to_pylist(),
                        arrow.column("uf").to_pylist()):
        if ibge is None:
            continue
        i = int(ibge)
        co6_to_ibge[i // 10] = i
        ibge_to_uf[i] = uf or "??"
    return co6_to_ibge, ibge_to_uf


def _parse_int(s: str) -> int:
    s = (s or "").strip().strip('"')
    if not s:
        return 0
    try:
        return int(s)
    except ValueError:
        return 0


def _tamanho_bucket(t: int) -> str | None:
    """RAIS tamanho codes → porte agregado."""
    if t in (1, 2):
        return "micro"          # 0-9 empregados
    if t in (3, 4):
        return "pequeno"        # 10-49
    if t in (5, 6):
        return "medio"          # 50-249
    if t in (7, 8, 9):
        return "grande"         # 250+
    return None


def _iter_ano(key: str, ano: int,
              co6_to_ibge: dict[int, int],
              ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """Stream 1 RAIS_ESTAB.7z → 1 row por (município, indicador)."""
    t0 = time.monotonic()
    raw = get_object_bytes(key)
    print(f"  RAIS {ano}: {len(raw)/1e6:.0f} MB raw ({time.monotonic()-t0:.1f}s)",
          file=sys.stderr)
    # Extract 7z pra tempdir
    t0 = time.monotonic()
    with tempfile.TemporaryDirectory() as td:
        with py7zr.SevenZipFile(io.BytesIO(raw)) as z:
            z.extractall(td)
        # 1 arquivo .COMT dentro
        # 2025+ usa .COMT; 2022 usa .txt.txt; outros podem variar
        csv_path = next(
            (os.path.join(td, n) for n in os.listdir(td)
             if n.upper().endswith((".COMT", ".TXT", ".CSV"))),
            None,
        )
        if not csv_path:
            print(f"  RAIS {ano}: nenhum .COMT/.TXT/.CSV no 7z (files={os.listdir(td)})",
                  file=sys.stderr)
            return
        print(f"  RAIS {ano}: extracted ({time.monotonic()-t0:.1f}s)",
              file=sys.stderr)

        t0 = time.monotonic()
        counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        sums: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        rows_seen = 0
        skip = 0
        # Auto-detect separator: 2022 usa ";", 2023-2025 usa ","
        with open(csv_path, encoding="latin-1") as fh:
            first = fh.readline()
        sep = ";" if first.count(";") > first.count(",") else ","
        with open(csv_path, encoding="latin-1") as fh:
            reader = csv.reader(fh, delimiter=sep)
            next(reader, None)  # header
            for row in reader:
                rows_seen += 1
                if len(row) < 21:
                    skip += 1
                    continue
                cm_raw = row[14].strip().strip('"')
                if not cm_raw or not cm_raw.isdigit():
                    skip += 1
                    continue
                co6 = int(cm_raw)
                ibge = co6_to_ibge.get(co6)
                if ibge is None:
                    skip += 1
                    continue
                vinc_ativos = _parse_int(row[7])
                vinc_clt = _parse_int(row[6])
                tamanho = _parse_int(row[18])
                counts[ibge]["total"] += 1
                if vinc_ativos > 0:
                    counts[ibge]["ativos"] += 1
                sums[ibge]["vinculos_ativos"] += vinc_ativos
                sums[ibge]["vinculos_clt"] += vinc_clt
                bucket = _tamanho_bucket(tamanho)
                if bucket:
                    counts[ibge][bucket] += 1
        print(f"  RAIS {ano}: {rows_seen:,} estabs, skip={skip:,} → "
              f"{len(counts):,} munis ({time.monotonic()-t0:.1f}s)",
              file=sys.stderr)

    periodo = str(ano)
    fonte_arq = f"RAIS_ESTAB_{ano}.7z"
    emitted = 0
    all_munis = set(counts) | set(sums)
    for ibge in all_munis:
        uf = ibge_to_uf.get(ibge, "??")
        for label, val in counts.get(ibge, {}).items():
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "rais",
                "indicador_id": f"rais.estabelecimentos_{label}",
                "periodo": periodo, "valor": float(val),
                "valor_texto": None, "unidade": "estabelecimentos",
                "fonte_arquivo": fonte_arq,
            }
            emitted += 1
        for label, val in sums.get(ibge, {}).items():
            if val == 0:
                continue
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "rais",
                "indicador_id": f"rais.{label}",
                "periodo": periodo, "valor": float(val),
                "valor_texto": None, "unidade": "vínculos",
                "fonte_arquivo": fonte_arq,
            }
            emitted += 1
    print(f"  RAIS {ano}: emitidos {emitted:,} rows", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ano", type=int, action="append")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("rais_estab_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://.../rais/raw/RAIS_ESTAB_*.7z já populado.",
                  file=sys.stderr)

    co6_to_ibge, ibge_to_uf = _build_co6_to_ibge_uf()
    print(f"RAIS-ESTAB→ibge dim: {len(co6_to_ibge):,} co6", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="rais")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def rais_estab() -> Iterator[dict]:
        anos_filter = set(args.ano) if args.ano else None
        for key in sorted(list_keys(S3_PREFIX)):
            name = key.rsplit("/", 1)[-1]
            m = FILE_RE.match(name)
            if not m:
                continue
            ano = int(m.group(1))
            if anos_filter and ano not in anos_filter:
                continue
            # Skip ano se TODOS os indicadores esperados já comitados
            expected = (
                "rais.estabelecimentos_total", "rais.estabelecimentos_ativos",
                "rais.vinculos_ativos", "rais.vinculos_clt",
                "rais.estabelecimentos_micro", "rais.estabelecimentos_pequeno",
                "rais.estabelecimentos_medio", "rais.estabelecimentos_grande",
            )
            if all(("rais", ind, str(ano)) in already for ind in expected):
                print(f"  RAIS {ano}: skip (todos indicadores já loaded)",
                      file=sys.stderr)
                continue
            for rec in _iter_ano(key, ano, co6_to_ibge, ibge_to_uf):
                if ("rais", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="rais_estab_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(rais_estab())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, periodo, count(*), avg(valor), sum(valor) "
                "FROM indicadores_serie GROUP BY indicador_id, periodo "
                "ORDER BY periodo, indicador_id LIMIT 30"))
        return 0

    pipe = dlt.pipeline(pipeline_name="rais_estab", destination=s3tables_iceberg)
    info = pipe.run(rais_estab())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
