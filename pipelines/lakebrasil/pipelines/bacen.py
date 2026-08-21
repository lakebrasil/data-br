"""BACEN BCB SGS séries → data_br.macro_serie via dlt.

End-to-end: chama `ensure_fetched("bacen_*")` no topo de `main()` para
baixar/atualizar todas as séries BCB declaradas em `catalog.yaml`
(IPCA, Selic, câmbio …) e em seguida roda o dlt para materialisar em
S3 Tables Iceberg. Substitui `_loaders/load_bacen.py` + o trecho de
macro_serie em `sqlite_to_iceberg.py`. Sem sqlite local.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bacen --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bacen --table macro_serie_dlt_test
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bacen           # → macro_serie (prod)
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bacen --no-fetch # pula download (raw já em disco)
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.bacen --refresh  # força re-download
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import PurePosixPath

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import max_value_per
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "bacen/raw/"
FILENAME_RE = re.compile(r"^(\d+)_(.+)\.json$")


def _parse_brdate(s: str) -> str:
    """BCB SGS devolve datas DD/MM/YYYY — converte pra ISO para que o
    DuckDB/Iceberg lidem com tipo DATE consistente (e ordene direito)."""
    return datetime.strptime(s, "%d/%m/%Y").date().isoformat()


@dlt.resource(
    name="macro_serie",
    primary_key=["serie_id", "data"],
    write_disposition="append",
)
def bacen_series() -> Iterator[dict]:
    """Itera JSONs em s3://{RAW_BUCKET}/bacen/raw/ → 1 linha por observação.

    Filtra `data > max(data) por serie_id` já carregado em Iceberg
    pra que re-runs sejam idempotentes (custom destination não dá pra
    fazer merge real; precisa filtrar aqui)."""
    high_water = max_value_per("macro_serie", "serie_id", "data")
    if high_water:
        sample = next(iter(high_water.items()))
        print(f"  bacen: incremental — {len(high_water)} séries já têm dados "
              f"(ex: serie {sample[0]} max_data={sample[1]})")

    for key in sorted(list_keys(S3_PREFIX)):
        name = PurePosixPath(key).name
        m = FILENAME_RE.match(name)
        if not m:
            continue
        serie_id, serie_nome = m.group(1), m.group(2)
        cutoff = high_water.get(serie_id)
        try:
            payload = json.loads(get_object_bytes(key))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            # BCB devolve `{"error": "..."}` quando série/range é inválido.
            continue
        n_yielded = 0
        for r in payload:
            data_iso = _parse_brdate(r["data"])
            if cutoff is not None and data_iso <= cutoff:
                continue  # já carregado em run anterior
            yield {
                "serie_id": serie_id,
                "serie_nome": serie_nome,
                "data": data_iso,
                "valor": float(str(r["valor"]).replace(",", ".")),
            }
            n_yielded += 1
        if cutoff and n_yielded == 0:
            continue  # série sem novidades


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="macro_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        # `bacen_*` no catalog.yaml cobre IPCA mensal, Selic diária, câmbio
        # dólar (e qualquer série bcb_sgs futura que use esse prefixo).
        ensure_fetched("bacen_*", refresh=args.refresh)

    if args.dry_run:
        # Stage local em DuckDB para inspeção, sem tocar no Iceberg.
        pipeline = dlt.pipeline(
            pipeline_name="bacen_dryrun",
            destination="duckdb",
            dataset_name="staging",
            dev_mode=True,  # nova DB a cada run; evita drift entre execuções
        )
        info = pipeline.run(bacen_series())
        print(info)
        with pipeline.sql_client() as client:
            stats = client.execute_sql(
                "SELECT count(*) AS rows, "
                "count(DISTINCT serie_id) AS series, "
                "min(data) AS min_data, max(data) AS max_data "
                "FROM macro_serie"
            )
            print("stage stats:", stats)
            sample = client.execute_sql(
                "SELECT serie_id, serie_nome, data, valor FROM macro_serie "
                "ORDER BY data DESC LIMIT 5"
            )
            for row in sample:
                print(" ", row)
        return 0

    # Real run via custom S3 Tables destination. A tabela alvo deve já
    # existir no namespace `data_br` (criada pelo DataBrS3TablesStack
    # no infra-cdk — schemas vivem lá).
    if args.table != "macro_serie":
        # Renomeia o resource pra alinhar com a tabela alvo (dlt usa
        # `resource.name` como `table["name"]` na destination).
        resource = bacen_series.with_name(args.table)
    else:
        resource = bacen_series

    pipeline = dlt.pipeline(
        pipeline_name=f"bacen_{args.table}",
        destination=s3tables_iceberg,
    )
    info = pipeline.run(resource)
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
