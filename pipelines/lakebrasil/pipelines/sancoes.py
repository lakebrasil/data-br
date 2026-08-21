"""CEIS + CNEP (Portal da Transparência) → data_br.sancoes via dlt.

End-to-end: `ensure_fetched(["ceis", "cnep"])` baixa os dumps daily-
refreshed do Portal (com tolerância a 404 = "ainda não publicado") e
em seguida o dlt materialisa em S3 Tables Iceberg.

Os dois cadastros têm 24-25 colunas idênticas exceto por `valor_multa`
(só CNEP). Vão pra mesma tabela Iceberg `sancoes` com `cadastro` =
'CEIS' | 'CNEP' como discriminador. `snapshot` carrega a data do dump
(extraída do nome do arquivo: `YYYYMMDD_CEIS.zip` → `YYYY-MM-DD`).

Encoding latin-1, separador ;, cabeçalho na linha 1.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sancoes --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sancoes --table sancoes_dlt_test
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sancoes
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sancoes --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sancoes --refresh
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.csv import parse_money_br, read_csv_records
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.filename import parse_yyyymmdd
from lakebrasil.common.incremental import loaded_pairs
from lakebrasil.common.s3 import list_keys, stream_zip_csv
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIXES = [
    ("CEIS", "ceis/raw/"),
    ("CNEP", "cnep/raw/"),
]

# Mapping é por header normalizado completo (não substring) para evitar
# colisões tipo "n" matchando "código da sanção". Adicionei TODAS as
# variantes que aparecem nos arquivos do Portal (CEIS e CNEP).
HEADER_TO_FIELD = {
    "cadastro":                                      "cadastro",
    "codigo da sancao":                              "codigo_sancao",
    "tipo de pessoa":                                "tipo_pessoa",
    "cpf ou cnpj do sancionado":                     "cpf_cnpj_sancionado",
    "nome do sancionado":                            "nome_sancionado",
    "nome informado pelo orgao sancionador":         "nome_orgao_sancionador",
    "razao social - cadastro receita":               "razao_social_rfb",
    "nome fantasia - cadastro receita":              "nome_fantasia_rfb",
    "numero do processo":                            "numero_processo",
    "categoria da sancao":                           "categoria_sancao",
    "valor da multa":                                "valor_multa",
    "data inicio sancao":                            "data_inicio_sancao",
    "data final sancao":                             "data_final_sancao",
    "data publicacao":                               "data_publicacao",
    "publicacao":                                    "publicacao",
    "detalhamento do meio de publicacao":            "detalhamento_publicacao",
    "data do transito em julgado":                   "data_transito_julgado",
    "abragencia da sancao":                          "abrangencia_sancao",   # typo no Portal: ABRAGENCIA
    "abrangencia da sancao":                         "abrangencia_sancao",
    "orgao sancionador":                             "orgao_sancionador",
    "uf orgao sancionador":                          "uf_orgao",
    "esfera orgao sancionador":                      "esfera_orgao",
    "fundamentacao legal":                           "fundamentacao_legal",
    "data origem informacao":                        "data_origem_info",
    "origem informacoes":                            "origem_info",
    "observacoes":                                   "observacoes",
}


def _yield_records(fh, cadastro: str, snapshot: str) -> Iterator[dict]:
    """Wrapper sobre `read_csv_records` que adiciona `valor_multa` cast
    + garante a coluna existe (CEIS não tem, CNEP tem)."""
    for rec in read_csv_records(
        fh, HEADER_TO_FIELD,
        extra_fields={"cadastro": cadastro, "snapshot": snapshot},
    ):
        if "valor_multa" in rec:
            rec["valor_multa"] = parse_money_br(rec["valor_multa"])
        else:
            rec["valor_multa"] = None
        yield rec


@dlt.resource(
    name="sancoes",
    primary_key=["cadastro", "codigo_sancao", "snapshot"],
    write_disposition="append",
)
def sancoes() -> Iterator[dict]:
    already = loaded_pairs("sancoes", "cadastro", "snapshot")
    if already:
        print(f"  sancoes: {len(already)} (cadastro,snapshot) já carregados — pulando")
    for cadastro, prefix in S3_PREFIXES:
        for key in sorted(list_keys(prefix)):
            name = key.rsplit("/", 1)[-1]
            snapshot = parse_yyyymmdd(name)
            if snapshot is None:
                continue
            if (cadastro, snapshot) in already:
                continue
            for _inner, fh in stream_zip_csv(key, csv_filter=lambda n: n.lower().endswith(".csv")):
                yield from _yield_records(fh, cadastro, snapshot)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="sancoes")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        # CEIS + CNEP são 2 entradas literais no catalog.yaml.
        ensure_fetched(["ceis", "cnep"], refresh=args.refresh)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="sancoes_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        info = pipe.run(sancoes())
        print(info)
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT cadastro, count(*) FROM sancoes GROUP BY cadastro"))
            print(c.execute_sql("SELECT * FROM sancoes LIMIT 3"))
        return 0

    resource = sancoes if args.table == "sancoes" else sancoes.with_name(args.table)
    pipe = dlt.pipeline(pipeline_name=f"sancoes_{args.table}", destination=s3tables_iceberg)
    info = pipe.run(resource)
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
