"""Emendas Parlamentares (Portal da Transparência) → data_br.emendas_parlamentares.

CSV latin-1, separador ;, JÁ traz Código IBGE — sem enrichment.
~92K linhas (snapshot único atual).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.emendas --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.emendas
"""
from __future__ import annotations

import argparse
import io
import sys
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.csv import parse_int_or_none, parse_money_br, read_csv_records
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_snapshots
from lakebrasil.common.s3 import RAW_BUCKET, get_object_bytes, s3_client
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_KEY = "emendas/raw/EmendasParlamentares.csv"

HEADER_TO_FIELD = {
    "codigo da emenda":                "codigo_emenda",
    "ano da emenda":                   "ano_emenda",
    "tipo de emenda":                  "tipo_emenda",
    "codigo do autor da emenda":       "codigo_autor",
    "nome do autor da emenda":         "nome_autor",
    "numero da emenda":                "numero_emenda",
    "localidade de aplicacao do recurso": "localidade_aplicacao",
    "codigo municipio ibge":           "ibge_code",
    "municipio":                       "municipio",
    "codigo uf ibge":                  "codigo_uf_ibge",
    "uf":                              "uf",
    "regiao":                          "regiao",
    "codigo funcao":                   "codigo_funcao",
    "nome funcao":                     "nome_funcao",
    "codigo subfuncao":                "codigo_subfuncao",
    "nome subfuncao":                  "nome_subfuncao",
    "codigo programa":                 "codigo_programa",
    "nome programa":                   "nome_programa",
    "codigo acao":                     "codigo_acao",
    "nome acao":                       "nome_acao",
    "codigo plano orcamentario":       "codigo_plano_orc",
    "nome plano orcamentario":         "nome_plano_orc",
    "valor empenhado":                 "valor_empenhado",
    "valor liquidado":                 "valor_liquidado",
    "valor pago":                      "valor_pago",
    "valor restos a pagar inscritos":  "valor_restos_inscritos",
    "valor restos a pagar cancelados": "valor_restos_cancelados",
    "valor restos a pagar pagos":      "valor_restos_pagos",
}

NUMERIC = {
    "ano_emenda", "ibge_code",
    "valor_empenhado", "valor_liquidado", "valor_pago",
    "valor_restos_inscritos", "valor_restos_cancelados", "valor_restos_pagos",
}


@dlt.resource(name="emendas_parlamentares", primary_key=["codigo_emenda"], write_disposition="append")
def emendas() -> Iterator[dict]:
    head = s3_client().head_object(Bucket=RAW_BUCKET, Key=S3_KEY)
    snapshot = head["LastModified"].date().isoformat()
    if snapshot in loaded_snapshots("emendas_parlamentares"):
        print(f"  emendas: snapshot {snapshot} já carregado — skip")
        return
    fh = io.BytesIO(get_object_bytes(S3_KEY))
    for rec in read_csv_records(fh, HEADER_TO_FIELD,
                                extra_fields={"snapshot": snapshot}):
        for k in ("ibge_code", "ano_emenda"):
            if k in rec:
                rec[k] = parse_int_or_none(rec[k])
        for k in NUMERIC - {"ibge_code", "ano_emenda"}:
            if k in rec:
                rec[k] = parse_money_br(rec[k])
        yield rec


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="emendas_parlamentares")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    if not args.no_fetch:
        ensure_fetched("emendas_parlamentares", refresh=args.refresh)
    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="emendas_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        info = pipe.run(emendas())
        print(info)
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT count(*), count(ibge_code), sum(valor_pago) "
                                "FROM emendas_parlamentares"))
            print(c.execute_sql("SELECT codigo_emenda, ano_emenda, uf, municipio, ibge_code, "
                                "valor_pago FROM emendas_parlamentares LIMIT 5"))
        return 0
    resource = emendas if args.table == "emendas_parlamentares" \
        else emendas.with_name(args.table)
    pipe = dlt.pipeline(pipeline_name=f"emendas_{args.table}", destination=s3tables_iceberg)
    info = pipe.run(resource)
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
