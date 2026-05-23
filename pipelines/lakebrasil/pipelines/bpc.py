"""BPC — Benefício de Prestação Continuada (LOAS) → indicadores_serie.

Lê `s3://data-br-raw/bpc/raw/{YYYY-MM}.zip` (Portal Transparência —
microdados mensais BPC, 1 row por beneficiário, ~5M rows/mês × ~130MB
ZIP). Agrega por município (CÓDIGO MUNICÍPIO SIAFI 4-dig → IBGE 7-dig
via siafi_to_ibge_map) em 3 indicadores × mês:

  bpc.beneficiarios       count(*) por município
  bpc.valor_total_mes     sum(VALOR PARCELA)   (R$)
  bpc.valor_medio_mes     avg(VALOR PARCELA)   (R$)

BPC = R$1.518/mês (1 salário mínimo, valor 2025) pra idosos 65+ e
pessoas com deficiência em famílias com renda per capita < ¼ salário.
~5M beneficiários no Brasil totalizando ~R$90 bi/ano.

Source: portaldatransparencia.gov.br/download-de-dados/bpc

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bpc --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bpc --ym 2025-03 --no-fetch
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import siafi_to_ibge_map, municipios_count
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "bpc/raw/"
# Aceita "2020-01.zip" (novo padrão) ou "202503_BPC.zip" (antigo)
FILE_RE = re.compile(r"^(?:(\d{4})-(\d{2})|(\d{4})(\d{2})_BPC)\.zip$")
PARALLEL_MONTHS = 4  # leitura S3 + parse paralelo entre meses


def _parse_money_br(s: str | None) -> float | None:
    if not s:
        return None
    s = s.strip().strip('"').replace(".", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _process_month(zip_key: str, ym: str,
                   siafi_map: dict[str, int],
                   ibge_to_uf: dict[int, str]) -> dict[int, tuple[int, float]]:
    """Stream CSV mensal → {ibge: (count, sum_valor)}. Roda em ThreadPool."""
    t0 = time.monotonic()
    raw = get_object_bytes(zip_key)
    accum: dict[int, list[float]] = defaultdict(lambda: [0, 0.0])  # [count, sum]
    rows = 0
    skip_siafi = 0
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        # Normalmente 1 csv só; pega o primeiro .csv
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            print(f"  BPC {ym}: nenhum .csv no zip", file=sys.stderr)
            return {}
        with zf.open(csv_name) as fh:
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="",
                                    errors="replace")
            reader = csv.reader(text, delimiter=";")
            header = next(reader, None)
            if not header:
                return {}
            # Localiza colunas por nome (tolera variações de acento/maiúscula)
            def find_col(*needles: str) -> int:
                for i, h in enumerate(header):
                    h_norm = (h or "").strip('"').strip().lower()
                    for n in needles:
                        if n.lower() in h_norm:
                            return i
                return -1
            col_siafi = find_col("c\xf3digo munic\xedpio siafi", "siafi")
            col_valor = find_col("valor parcela")
            if col_siafi < 0 or col_valor < 0:
                print(f"  BPC {ym}: header inesperado (siafi={col_siafi} "
                      f"valor={col_valor})", file=sys.stderr)
                return {}
            for row in reader:
                rows += 1
                if len(row) <= max(col_siafi, col_valor):
                    continue
                siafi_raw = row[col_siafi].strip('"').strip()
                if not siafi_raw:
                    skip_siafi += 1
                    continue
                # SIAFI vem como string 4-dig "0643"; map key é string.
                ibge = siafi_map.get(siafi_raw)
                if ibge is None:
                    # tenta sem zero à esquerda
                    ibge = siafi_map.get(siafi_raw.lstrip("0"))
                if ibge is None:
                    skip_siafi += 1
                    continue
                valor = _parse_money_br(row[col_valor]) or 0.0
                accum[ibge][0] += 1
                accum[ibge][1] += valor
    print(f"  BPC {ym}: {rows:,} beneficiários, skip_siafi={skip_siafi:,} → "
          f"{len(accum):,} munis em {time.monotonic()-t0:.1f}s", file=sys.stderr)
    return {ibge: tuple(v) for ibge, v in accum.items()}


def _iter_records(month_data: dict[str, dict[int, tuple[int, float]]],
                  ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """Converte accumulators dos meses em rows pra indicadores_serie."""
    for ym, accum in month_data.items():
        for ibge, (count, sum_valor) in accum.items():
            uf = ibge_to_uf.get(ibge, "??")
            avg = sum_valor / count if count else 0.0
            for ind_suffix, valor, unidade in (
                ("beneficiarios",  float(count),  "pessoas"),
                ("valor_total_mes", sum_valor,    "R$"),
                ("valor_medio_mes", avg,          "R$"),
            ):
                yield {
                    "ibge_code":     ibge,
                    "uf":            uf,
                    "fonte":         "bpc",
                    "indicador_id":  f"bpc.{ind_suffix}",
                    "periodo":       ym,  # "YYYY-MM"
                    "valor":         valor,
                    "valor_texto":   None,
                    "unidade":       unidade,
                    "fonte_arquivo": f"{ym}.zip",
                }


def _build_ibge_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    return {int(i): u or "??" for i, u in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
    ) if i is not None}


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ym", action="append",
                   help="Lista de YYYY-MM a carregar (default: todos no raw).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("bpc_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/bpc/raw/ já populado.",
                  file=sys.stderr)

    municipios_count()
    siafi_map = siafi_to_ibge_map()
    print(f"BPC→ibge dim: {len(siafi_map):,} SIAFI codes", file=sys.stderr)
    ibge_to_uf = _build_ibge_to_uf()

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="bpc")

    # Lista todos os arquivos elegíveis
    ym_to_key: dict[str, str] = {}
    for key in sorted(list_keys(S3_PREFIX)):
        name = key.rsplit("/", 1)[-1]
        m = FILE_RE.match(name)
        if not m:
            continue
        ym = (f"{m.group(1)}-{m.group(2)}" if m.group(1)
              else f"{m.group(3)}-{m.group(4)}")
        ym_to_key[ym] = key
    if args.ym:
        ym_to_key = {y: k for y, k in ym_to_key.items() if y in args.ym}
    # Pula meses cujos 3 indicadores já estão todos commitados
    expected_inds = ("bpc.beneficiarios", "bpc.valor_total_mes",
                     "bpc.valor_medio_mes")
    pending = {
        ym: k for ym, k in ym_to_key.items()
        if any(("bpc", ind, ym) not in already for ind in expected_inds)
    }
    print(f"BPC: {len(ym_to_key)} meses raw, {len(pending)} pendentes "
          f"(workers={PARALLEL_MONTHS})", file=sys.stderr)

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def bpc_indicadores() -> Iterator[dict]:
        # Paraleliza extract (S3 + zip + CSV stream) entre meses; agrega
        # em memória e depois yield sequencial.
        month_data: dict[str, dict[int, tuple[int, float]]] = {}
        with ThreadPoolExecutor(max_workers=PARALLEL_MONTHS) as pool:
            futures = {
                pool.submit(_process_month, key, ym, siafi_map, ibge_to_uf): ym
                for ym, key in pending.items()
            }
            for fut in as_completed(futures):
                ym = futures[fut]
                month_data[ym] = fut.result()
        yield from _iter_records(month_data, ibge_to_uf)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="bpc_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(bpc_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, periodo, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id, periodo "
                "ORDER BY periodo, indicador_id LIMIT 30"))
        return 0

    pipe = dlt.pipeline(pipeline_name="bpc", destination=s3tables_iceberg)
    info = pipe.run(bpc_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
