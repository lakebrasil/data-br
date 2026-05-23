"""MEC Pé-de-Meia → indicadores_serie.

Programa Pé-de-Meia (incentivo financeiro pra alunos ensino médio
em risco de evasão). Lê `s3://data-br-raw/mec/raw/pe-de-meia_{YYYYMM}.zip`
(Portal Transparência, 1 row/beneficiário-pagamento, ~90MB ZIP).

Schema é similar a BPC (mesma SIAFI 4-dig + VALOR PARCELA) — reusa
`_process_month` do BPC para extração. Diferenças:
  - Beneficiários do programa Pé-de-meia (estudantes 14-24 anos)
  - Múltiplos tipos de incentivo (matrícula, frequência, ENEM)
  - Múltiplas etapas de ensino

Emite 3 indicadores × mês:
  pdm.beneficiarios     count beneficiários (pode haver duplicados se
                         um estudante recebeu múltiplos incentivos no mês)
  pdm.valor_total_mes   sum(VALOR PARCELA) R$
  pdm.valor_medio_mes   avg(VALOR PARCELA) R$

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.pe_de_meia --no-fetch
"""
from __future__ import annotations

import argparse
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import siafi_to_ibge_map, municipios_count
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import list_keys
from lakebrasil.pipelines.bpc import _process_month, _build_ibge_to_uf
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "mec/raw/"
FILE_RE = re.compile(r"^pe-de-meia_(\d{4})(\d{2})\.zip$")
PARALLEL_MONTHS = 4
FONTE = "pdm"


def _iter_records(month_data: dict[str, dict[int, tuple[int, float]]],
                  ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    for ym, accum in month_data.items():
        for ibge, (count, sum_valor) in accum.items():
            uf = ibge_to_uf.get(ibge, "??")
            avg = sum_valor / count if count else 0.0
            for ind_suffix, valor, unidade in (
                ("beneficiarios",  float(count),  "pagamentos"),
                ("valor_total_mes", sum_valor,    "R$"),
                ("valor_medio_mes", avg,          "R$"),
            ):
                yield {
                    "ibge_code":     ibge,
                    "uf":            uf,
                    "fonte":         FONTE,
                    "indicador_id":  f"{FONTE}.{ind_suffix}",
                    "periodo":       ym,
                    "valor":         valor,
                    "valor_texto":   None,
                    "unidade":       unidade,
                    "fonte_arquivo": f"pe-de-meia_{ym.replace('-', '')}.zip",
                }


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ym", action="append", help="Lista YYYY-MM.")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    if not args.no_fetch:
        try:
            ensure_fetched("pe_de_meia_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/mec/raw/pe-de-meia_*.zip já populado.",
                  file=sys.stderr)

    municipios_count()
    siafi_map = siafi_to_ibge_map()
    ibge_to_uf = _build_ibge_to_uf()

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte=FONTE)

    ym_to_key: dict[str, str] = {}
    for key in sorted(list_keys(S3_PREFIX)):
        name = key.rsplit("/", 1)[-1]
        m = FILE_RE.match(name)
        if not m:
            continue
        ym = f"{m.group(1)}-{m.group(2)}"
        ym_to_key[ym] = key
    if args.ym:
        ym_to_key = {y: k for y, k in ym_to_key.items() if y in args.ym}
    expected = (f"{FONTE}.beneficiarios", f"{FONTE}.valor_total_mes",
                f"{FONTE}.valor_medio_mes")
    pending = {ym: k for ym, k in ym_to_key.items()
               if any((FONTE, i, ym) not in already for i in expected)}
    print(f"Pé-de-meia: {len(ym_to_key)} meses raw, {len(pending)} pendentes",
          file=sys.stderr)

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def pdm_indicadores() -> Iterator[dict]:
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
        pipe = dlt.pipeline(pipeline_name="pdm_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(pdm_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, periodo, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id, periodo "
                "ORDER BY periodo, indicador_id"))
        return 0

    pipe = dlt.pipeline(pipeline_name="pe_de_meia", destination=s3tables_iceberg)
    info = pipe.run(pdm_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
