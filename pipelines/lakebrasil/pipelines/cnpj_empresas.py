"""CNPJ Empresas × Estabelecimentos → data_br.empresas_municipio_cnae.

Agrega o dump RFB Pessoa Jurídica por (município, uf, cnae_principal,
porte_empresa) → COUNT(estabelecimentos). Phase E da ingestão CNPJ:
adiciona dimensão `porte` que `cnpj_estabelecimentos_municipio` não tem
(esse último agrega por situação cadastral, não porte).

Pass 1: stream todos Empresas{N}.zip → dict {cnpj_basico (8 chars) → porte}
        Porte codes RFB:
          '00' = Não informado
          '01' = Micro Empresa (ME)
          '03' = Empresa Pequeno Porte (EPP)
          '05' = Demais (médio/grande)

Pass 2: stream todos Estabelecimentos{N}.zip → para cada row, lookup
        porte via cnpj_basico, agrega no Counter compartilhado por
        (uf, municipio_rf, cnae_principal, porte).

Pass 3: emit rows com IBGE resolvido (rf_to_ibge via municipios slug match).

Memória pico: ~55M empresas × ~30 bytes = ~1.7 GB no dict.
Fargate task com 16 GB comporta — pra rodar local ATENÇÃO ao limite.

Mapeamento → EMPRESAS_SCHEMA:
  ibge_code  ← rf_to_ibge[municipio_rf]
  uf         ← Estabelecimentos.uf
  cnae       ← Estabelecimentos.cnae_principal
  porte      ← Empresas.porte_empresa  ('00'|'01'|'03'|'05')
  qtd        ← COUNT(*)
  snapshot   ← --snapshot YYYY-MM

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.cnpj_empresas --snapshot 2026-04 --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.cnpj_empresas --snapshot 2026-04 --limit 1 --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import zipfile
from collections import Counter
from typing import Iterator

import boto3
import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.s3 import RAW_BUCKET, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

# Importa do cnpj.py os índices RFB→IBGE + constantes
from lakebrasil.pipelines.cnpj import (
    ESTAB_COLS,
    ESTAB_FILE_RE,
    _build_rf_to_ibge,
)
from lakebrasil.common.incremental import loaded_snapshots

S3_PREFIX = "cnpj/raw/"
EMPRESAS_FILE_RE = re.compile(r"^Empresas(\d+)\.zip$")

# Empresas CSV layout RFB (sem header, 7 cols, latin-1 ; quotechar "):
#   0=cnpj_basico, 1=razao_social, 2=natureza_juridica, 3=qualif_resp,
#   4=capital_social, 5=porte_empresa, 6=ente_federativo_resp
EMP_COL_CNPJ_BASICO = 0
EMP_COL_PORTE = 5

# Estabelecimentos CSV → cnpj_basico está na coluna 0 (igual Empresas).
# Os outros campos vêm do ESTAB_COLS importado.


def _build_porte_index(estab_keys_count: int) -> dict[str, str]:
    """Pass 1: percorre todas Empresas{N}.zip → {cnpj_basico: porte}.

    Volume típico: ~55M empresas. Dict resultante ~1.5 GB com PyObject
    overhead. Pra reduzir, valores são str de 2 chars ('00'-'05') —
    Python intern automaticamente strings curtas; cnpj_basico é o
    cost dominante (8 bytes × 55M ~ 440 MB pra strings).
    """
    print(f"PASS 1: building porte index dos Empresas zips...", file=sys.stderr)
    s3 = boto3.client("s3")
    keys = sorted(list_keys(S3_PREFIX))
    empresas_keys = [k for k in keys
                     if EMPRESAS_FILE_RE.match(k.rsplit("/", 1)[-1])]
    print(f"  encontrou {len(empresas_keys)} Empresas zips", file=sys.stderr)
    porte_idx: dict[str, str] = {}
    for key in empresas_keys:
        t0 = time.monotonic()
        name = key.rsplit("/", 1)[-1]
        body = s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
        n = 0
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            for inner in zf.namelist():
                if inner.endswith("/"):
                    continue
                with zf.open(inner) as fh:
                    text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="")
                    reader = csv.reader(text_io, delimiter=";", quotechar='"')
                    for row in reader:
                        if len(row) <= EMP_COL_PORTE:
                            continue
                        cnpj_basico = (row[EMP_COL_CNPJ_BASICO] or "").strip()
                        porte = (row[EMP_COL_PORTE] or "").strip()
                        if not cnpj_basico:
                            continue
                        porte_idx[cnpj_basico] = porte
                        n += 1
                        if n % 1_000_000 == 0:
                            print(f"    {name}: {n:,} rows lidas", file=sys.stderr)
        print(f"  PASS1 {name}: total={n:,} index_size={len(porte_idx):,} "
              f"em {time.monotonic()-t0:.1f}s", file=sys.stderr)
    return porte_idx


def _aggregate_estab_with_porte(
    s3_key: str,
    accumulator: Counter[tuple[str, str, str, str]],
    porte_idx: dict[str, str],
) -> int:
    """Pass 2: stream Estabelecimentos zip → soma em accumulator
    (uf, municipio_rf, cnae_principal, porte).

    Empresas com cnpj_basico não encontrado em porte_idx recebem porte=''
    (sentinel — vira string vazia no Iceberg). Ratio típico de match
    deve ser ~100% pois ambos vêm do mesmo snapshot RFB.
    """
    s3 = boto3.client("s3")
    name = s3_key.rsplit("/", 1)[-1]
    body = s3.get_object(Bucket=RAW_BUCKET, Key=s3_key)["Body"].read()
    rows_seen = 0
    miss_porte = 0
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        for inner in zf.namelist():
            if inner.endswith("/"):
                continue
            with zf.open(inner) as fh:
                text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="")
                reader = csv.reader(text_io, delimiter=";", quotechar='"')
                for row in reader:
                    if len(row) <= ESTAB_COLS["municipio_rf"]:
                        continue
                    uf = (row[ESTAB_COLS["uf"]] or "").strip()
                    if not uf:
                        continue
                    cnpj_basico = (row[0] or "").strip()  # mesma posição que Empresas
                    porte = porte_idx.get(cnpj_basico, "")
                    if not porte:
                        miss_porte += 1
                    accumulator[(
                        uf,
                        (row[ESTAB_COLS["municipio_rf"]] or "").strip(),
                        (row[ESTAB_COLS["cnae_principal"]] or "").strip(),
                        porte,
                    )] += 1
                    rows_seen += 1
                    if rows_seen % 1_000_000 == 0:
                        print(f"    PASS2 {name}: {rows_seen:,} rows "
                              f"({len(accumulator):,} chaves, "
                              f"{miss_porte:,} miss porte)", file=sys.stderr)
    print(f"  PASS2 {name}: total={rows_seen:,} miss_porte={miss_porte:,} "
          f"(acumulador agora {len(accumulator):,} chaves)", file=sys.stderr)
    return rows_seen


def _emit_aggregated(
    counter: Counter[tuple[str, str, str, str]],
    snapshot: str,
    rf_to_ibge: dict[str, int],
) -> Iterator[dict]:
    """Pass 3: materializa o counter em rows pra dlt. Skipa rows sem
    ibge match (schema empresas_municipio_cnae requer ibge_code NOT NULL)."""
    skipped_ibge = 0
    skipped_cnae = 0
    skipped_porte = 0
    emitted = 0
    for (uf, municipio_rf, cnae, porte), qtd in counter.items():
        ibge = rf_to_ibge.get(municipio_rf) if municipio_rf else None
        if ibge is None:
            skipped_ibge += 1
            continue
        if not cnae:
            skipped_cnae += 1
            continue
        if not porte:
            skipped_porte += 1
            continue
        yield {
            "ibge_code":  ibge,
            "uf":         uf,
            "cnae":       cnae,
            "porte":      porte,
            "qtd":        qtd,
            "snapshot":   snapshot,
        }
        emitted += 1
    print(f"  PASS3 emit: {emitted:,} rows (skip ibge={skipped_ibge:,} "
          f"cnae={skipped_cnae:,} porte={skipped_porte:,})", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--snapshot", required=True,
                   help="YYYY-MM do dump RFB.")
    p.add_argument("--limit", type=int,
                   help="Processa só os primeiros N zips (Empresas + Estab) — debug.")
    add_common_args(p, include_table=False)
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("cnpj_*", refresh=args.refresh)
        except ValueError as e:
            print(f"  [warn] {e}", file=sys.stderr)
            print("  [warn] assumindo s3://data-br-raw/cnpj/raw/ já populado.",
                  file=sys.stderr)

    snapshot = args.snapshot
    already = set() if args.full_refresh else loaded_snapshots(
        "empresas_municipio_cnae"
    )
    if snapshot in already:
        print(f"snapshot {snapshot} já em Iceberg — usar --full-refresh "
              f"pra reprocessar.", file=sys.stderr)
        return 0

    rf_to_ibge = _build_rf_to_ibge(snapshot)
    print(f"RFB→IBGE índice: {len(rf_to_ibge):,} municípios", file=sys.stderr)

    # ── PASS 1: index Empresas → porte
    keys = sorted(list_keys(S3_PREFIX))
    empresas_keys = [k for k in keys if EMPRESAS_FILE_RE.match(k.rsplit("/", 1)[-1])]
    estab_keys = [k for k in keys if ESTAB_FILE_RE.match(k.rsplit("/", 1)[-1])]
    if args.limit:
        empresas_keys = empresas_keys[: args.limit]
        estab_keys = estab_keys[: args.limit]
    porte_idx = _build_porte_index(len(estab_keys))

    # ── PASS 2: stream Estabelecimentos + JOIN
    @dlt.resource(name="empresas_municipio_cnae", write_disposition="append")
    def emp_munis_cnae() -> Iterator[dict]:
        accumulator: Counter[tuple[str, str, str, str]] = Counter()
        for key in estab_keys:
            t0 = time.monotonic()
            n = _aggregate_estab_with_porte(key, accumulator, porte_idx)
            print(f"  PASS2 {key.rsplit('/',1)[-1]}: {n:,} rows em "
                  f"{time.monotonic()-t0:.1f}s", file=sys.stderr)
        print(f"  TOTAL acumulado: {len(accumulator):,} chaves "
              f"(uf,mun,cnae,porte)", file=sys.stderr)
        yield from _emit_aggregated(accumulator, snapshot, rf_to_ibge)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="cnpj_empresas_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(emp_munis_cnae())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT porte, count(*) AS distinct_keys, sum(qtd) AS empresas "
                "FROM empresas_municipio_cnae GROUP BY porte ORDER BY porte"))
        return 0

    pipe = dlt.pipeline(pipeline_name="cnpj_empresas", destination=s3tables_iceberg)
    info = pipe.run(emp_munis_cnae())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
