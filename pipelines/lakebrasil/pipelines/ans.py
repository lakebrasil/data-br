"""ANS — Beneficiários de planos de saúde → indicadores_serie.

Lê `s3://data-br-raw/ans/raw/pda-024-icb-{UF}-{YYYY_MM}.zip` (Agência
Nacional de Saúde Suplementar — base ICB por UF/mês). 325 zips × ~5MB
CSV cada = 27 UFs × 13 meses.

Schema (1 row por contrato-beneficiário):
  ID_CMPT_MOVEL          YYYY-MM mês competência
  CD_MUNICIPIO           IBGE 6-dig truncado (igual SIOPE/SNIS)
  COBERTURA_ASSIST_PLAN  "Médico-Hospitalar" | "Odontológico"
  QT_BENEFICIARIO_ATIVO  count ativos no mês

Agrega por (município, mês) em 3 indicadores:
  ans.beneficiarios_total
  ans.beneficiarios_medico_hospitalar
  ans.beneficiarios_odontologico

ThreadPoolExecutor(6) paraleliza zips entre UF/mês.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.ans --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.ans --uf AC --ym 2024-04 --no-fetch
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
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "ans/raw/"
FILE_RE = re.compile(r"^pda-024-icb-([A-Z]{2})-(\d{4})_(\d{2})\.zip$")
PARALLEL_FILES = 6


def _build_co6_to_ibge_uf() -> tuple[dict[int, int], dict[int, str]]:
    """6-dig truncado → (7-dig IBGE, UF sigla)."""
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


def _process_file(key: str, uf: str, ym: str,
                  co6_to_ibge: dict[int, int]) -> dict[int, dict[str, int]]:
    """Stream 1 ZIP/UF/mês → {ibge: {total, medico_hospitalar, odontologico}}."""
    t0 = time.monotonic()
    raw = get_object_bytes(key)
    accum: dict[int, dict[str, int]] = defaultdict(
        lambda: defaultdict(int))
    rows_seen = skip = 0
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
        if not csv_name:
            return {}
        with zf.open(csv_name) as fh:
            text = io.TextIOWrapper(fh, encoding="utf-8", newline="",
                                    errors="replace")
            reader = csv.DictReader(text, delimiter=";")
            for r in reader:
                rows_seen += 1
                cm = (r.get("CD_MUNICIPIO") or "").strip().strip('"')
                if not cm or not cm.isdigit():
                    skip += 1
                    continue
                co6 = int(cm)
                ibge = co6_to_ibge.get(co6)
                if ibge is None:
                    skip += 1
                    continue
                qt_raw = (r.get("QT_BENEFICIARIO_ATIVO") or "0").strip().strip('"')
                try:
                    qt = int(qt_raw or "0")
                except ValueError:
                    qt = 0
                if qt == 0:
                    continue
                accum[ibge]["total"] += qt
                cobertura = (r.get("COBERTURA_ASSIST_PLAN")
                             or "").strip().strip('"').lower()
                if "odonto" in cobertura:
                    accum[ibge]["odontologico"] += qt
                else:
                    # default: médico-hospitalar (inclui "Médico-Hospitalar",
                    # "Médico e Hospitalar" e variantes)
                    accum[ibge]["medico_hospitalar"] += qt
    if rows_seen:
        print(f"  ANS {uf}/{ym}: {rows_seen:,} rows, skip={skip} → "
              f"{len(accum)} munis em {time.monotonic()-t0:.1f}s",
              file=sys.stderr)
    return {ibge: dict(d) for ibge, d in accum.items()}


def _iter_records(file_data: dict[tuple[str, str], dict[int, dict[str, int]]],
                  ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """file_data: (uf, ym) → {ibge: {bucket: count}} → indicadores_serie rows.

    Soma sobre UFs (algumas operadoras cruzam UF): combina per (ibge, ym)."""
    combined: dict[tuple[int, str], dict[str, int]] = defaultdict(
        lambda: defaultdict(int))
    for (uf, ym), per_ibge in file_data.items():
        for ibge, buckets in per_ibge.items():
            for bk, v in buckets.items():
                combined[(ibge, ym)][bk] += v
    for (ibge, ym), buckets in combined.items():
        uf = ibge_to_uf.get(ibge, "??")
        for bk, v in buckets.items():
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "ans",
                "indicador_id": f"ans.beneficiarios_{bk}",
                "periodo": ym, "valor": float(v),
                "valor_texto": None, "unidade": "pessoas",
                "fonte_arquivo": f"pda-024-icb-{ym.replace('-','_')}.zip",
            }


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--uf", action="append", help="Filtra por UF (sigla).")
    p.add_argument("--ym", action="append", help="Filtra por YYYY-MM.")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    if not args.no_fetch:
        try:
            ensure_fetched("ans_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://.../ans/raw/ já populado.",
                  file=sys.stderr)

    co6_to_ibge, ibge_to_uf = _build_co6_to_ibge_uf()
    print(f"ANS→ibge dim: {len(co6_to_ibge):,} co6 codes", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="ans")

    # Enumera arquivos
    jobs: list[tuple[str, str, str, str]] = []  # (key, uf, ym, key_again)
    for key in sorted(list_keys(S3_PREFIX)):
        name = key.rsplit("/", 1)[-1]
        m = FILE_RE.match(name)
        if not m:
            continue
        uf, ano, mes = m.group(1), m.group(2), m.group(3)
        ym = f"{ano}-{mes}"
        if args.uf and uf not in set(args.uf):
            continue
        if args.ym and ym not in set(args.ym):
            continue
        jobs.append((key, uf, ym, name))

    # Pre-filter: pula meses cujos 3 indicadores TODOS já estão loaded
    # (granularidade ym; se algum bucket ainda não foi salvo, re-roda mês inteiro)
    expected = ("ans.beneficiarios_total", "ans.beneficiarios_medico_hospitalar",
                "ans.beneficiarios_odontologico")
    pending_yms = {ym for _, _, ym, _ in jobs
                   if any(("ans", ind, ym) not in already for ind in expected)}
    pending_jobs = [j for j in jobs if j[2] in pending_yms]
    print(f"ANS: {len(jobs)} arquivos, {len(pending_jobs)} pendentes "
          f"({len(pending_yms)} meses únicos)", file=sys.stderr)

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def ans_indicadores() -> Iterator[dict]:
        file_data: dict[tuple[str, str], dict[int, dict[str, int]]] = {}
        with ThreadPoolExecutor(max_workers=PARALLEL_FILES) as pool:
            futures = {
                pool.submit(_process_file, key, uf, ym, co6_to_ibge):
                    (uf, ym)
                for key, uf, ym, _ in pending_jobs
            }
            done = 0
            for fut in as_completed(futures):
                done += 1
                uf, ym = futures[fut]
                file_data[(uf, ym)] = fut.result()
                if done % 20 == 0 or done == len(pending_jobs):
                    print(f"    ANS: {done}/{len(pending_jobs)} arquivos",
                          file=sys.stderr)
        yield from _iter_records(file_data, ibge_to_uf)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="ans_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(ans_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, periodo, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id, periodo "
                "ORDER BY periodo, indicador_id LIMIT 20"))
        return 0

    pipe = dlt.pipeline(pipeline_name="ans", destination=s3tables_iceberg)
    info = pipe.run(ans_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
