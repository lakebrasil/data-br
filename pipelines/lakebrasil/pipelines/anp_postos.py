"""ANP revendedores varejistas → data_br.anp_postos via dlt.

End-to-end: `ensure_fetched("anp_postos")` baixa o CSV via fetcher
`govbr` (urllib UA `Python-urllib/3.13` para passar pelo F5 BIG-IP) e
em seguida o dlt materialisa em Iceberg.

CSV latin-1, separador ;, cabeçalho:
    CODIGOISIMP;AUTORIZACAO;DATAPUBLICACAO;RAZAOSOCIAL;CNPJ;ENDERECO;
    COMPLEMENTO;BAIRRO;CEP;UF;MUNICIPIO;BANDEIRA;DATAVINCULACAO

Enrichment: `ibge_code` resolvido no pipeline via `_enrich.resolve_ibge`
(slug match em municipios). Match rate é reportado no fim.

Snapshot: usa o mtime do arquivo CSV como dump-date (não há campo no CSV).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.anp_postos --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.anp_postos
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.anp_postos --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.anp_postos --refresh
"""
from __future__ import annotations

import argparse
import io
import sys
from typing import Iterator

import boto3
import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.csv import read_csv_records
from lakebrasil.common.incremental import loaded_snapshots

from lakebrasil.common.enrich import resolve_ibge, municipios_count
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.s3 import RAW_BUCKET, get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "anp/raw/"

# Use o arquivo mais novo entre os 2 de revendedores (sometimes there
# are duplicate snapshots like dados-cadastrais-* and postos_revendedores).
CANDIDATES = [
    "postos_revendedores.csv",
    "dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv",
]


def _pick_source_key() -> str | None:
    keys = set(list_keys(S3_PREFIX))
    for name in CANDIDATES:
        k = f"{S3_PREFIX}{name}"
        if k in keys:
            return k
    return None


# Header CSV → coluna canônica. Keys lowercase porque `read_csv_records`
# usa `normalize_header` que minusculiza.
HEADER_TO_FIELD = {
    "codigoisimp":      "codigo_isimp",
    "autorizacao":      "autorizacao",
    "datapublicacao":   "data_publicacao",
    "razaosocial":      "razao_social",
    "cnpj":             "cnpj",
    "endereco":         "endereco",
    "complemento":      "complemento",
    "bairro":           "bairro",
    "cep":              "cep",
    "uf":               "uf",
    "municipio":        "municipio",
    "bandeira":         "bandeira",
    "datavinculacao":   "data_vinculacao",
}


_stats = {"total": 0, "ibge_resolved": 0, "ibge_missing_examples": []}


@dlt.resource(name="anp_postos", primary_key=["cnpj", "snapshot"], write_disposition="append")
def anp_postos() -> Iterator[dict]:
    src_key = _pick_source_key()
    if src_key is None:
        return
    head = boto3.client("s3").head_object(Bucket=RAW_BUCKET, Key=src_key)
    snapshot = head["LastModified"].date().isoformat()

    if snapshot in loaded_snapshots("anp_postos"):
        print(f"  anp_postos: snapshot {snapshot} já carregado — skip")
        return

    municipios_count()  # warm up lru_cache do dim
    fh = io.BytesIO(get_object_bytes(src_key))
    for rec in read_csv_records(fh, HEADER_TO_FIELD,
                                extra_fields={"snapshot": snapshot}):
        ibge = resolve_ibge(rec.get("uf"), rec.get("municipio"))
        rec["ibge_code"] = ibge
        _stats["total"] += 1
        if ibge is not None:
            _stats["ibge_resolved"] += 1
        elif (rec.get("uf") and rec.get("municipio")
              and len(_stats["ibge_missing_examples"]) < 10):
            _stats["ibge_missing_examples"].append(
                f"{rec['uf']}/{rec['municipio']}")
        yield rec


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="anp_postos")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        ensure_fetched("anp_postos", refresh=args.refresh)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="anp_postos_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        info = pipe.run(anp_postos())
        print(info)
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT count(*), count(ibge_code), "
                                "count(*) - count(ibge_code) AS missing FROM anp_postos"))
            print(c.execute_sql("SELECT cnpj, razao_social, uf, municipio, ibge_code "
                                "FROM anp_postos LIMIT 5"))
    else:
        resource = anp_postos if args.table == "anp_postos" \
            else anp_postos.with_name(args.table)
        pipe = dlt.pipeline(pipeline_name=f"anp_postos_{args.table}",
                            destination=s3tables_iceberg)
        info = pipe.run(resource)
        print(info)

    print(f"\nibge resolution: {_stats['ibge_resolved']:,}/{_stats['total']:,} "
          f"({100*_stats['ibge_resolved']/max(_stats['total'],1):.1f}%)")
    if _stats["ibge_missing_examples"]:
        print("first unresolved (uf/municipio):")
        for x in _stats["ibge_missing_examples"]:
            print(f"  - {x}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
