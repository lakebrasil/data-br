"""Orçamento despesas execução (Portal da Transparência) → orcamento_execucao_municipal.

Cada arquivo mensal traz despesas FEDERAIS por linha (~48K/mês, 86 meses
= 4M linhas). Agregamos por (mes, ibge, funcao, subfuncao, programa)
via DuckDB pra reduzir pra ~poucos milhões agregados úteis.

Match ibge_code via municipios.slug normalizado de (UF, Município) string.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.orcamento --month 202401 --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.orcamento
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterator

import boto3
import dlt
import duckdb

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import _slug
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.s3 import RAW_BUCKET, list_keys, local_path
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "orcamento/raw/"
FILE_RE = re.compile(r"^(\d{6})_despesas-execucao\.zip$")


def _build_uf_slug_map() -> dict[tuple[str, str], int]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf", "slug")
    ).to_arrow()
    out: dict[tuple[str, str], int] = {}
    for ibge, uf, slug in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
        arrow.column("slug").to_pylist(),
    ):
        if uf and slug:
            out[(uf.upper(), slug)] = int(ibge)
    return out


def _aggregate_month(s3_key: str, mes_yyyymm: str,
                     uf_slug_to_ibge: dict[tuple[str, str], int]) -> Iterator[dict]:
    mount_zip = local_path(s3_key)
    tmp_zip: Path | None = None
    tmp_csv = Path("/tmp") / f"orcamento_{mes_yyyymm}.csv"
    try:
        if mount_zip is not None:
            zip_to_read = mount_zip
        else:
            tmp_zip = Path("/tmp") / f"orcamento_{mes_yyyymm}.zip"
            boto3.client("s3").download_file(RAW_BUCKET, s3_key, str(tmp_zip))
            zip_to_read = tmp_zip

        with zipfile.ZipFile(zip_to_read) as zf:
            csv_inner = next((n for n in zf.namelist() if n.endswith(".csv")), None)
            if not csv_inner:
                return
            with zf.open(csv_inner) as src, tmp_csv.open("wb") as dst:
                while True:
                    b = src.read(1 << 20)
                    if not b:
                        break
                    dst.write(b)
        if tmp_zip is not None:
            try:
                tmp_zip.unlink()
                tmp_zip = None
            except FileNotFoundError:
                pass
        tmp = tmp_csv

        mes_iso = f"{mes_yyyymm[:4]}-{mes_yyyymm[4:6]}"
        con = duckdb.connect()
        sql = f"""
            SELECT
                "UF" AS uf,
                "Munic\xedpio" AS municipio,
                "C\xf3digo Fun\xe7\xe3o" AS codigo_funcao,
                "Nome Fun\xe7\xe3o" AS nome_funcao,
                "C\xf3digo Subfu\xe7\xe3o" AS codigo_subfuncao,
                "Nome Subfun\xe7\xe3o" AS nome_subfuncao,
                "C\xf3digo Programa Or\xe7ament\xe1rio" AS codigo_programa,
                SUM(TRY_CAST(REPLACE("Valor Empenhado (R$)", ',', '.') AS DOUBLE)) AS valor_empenhado,
                SUM(TRY_CAST(REPLACE("Valor Liquidado (R$)", ',', '.') AS DOUBLE)) AS valor_liquidado,
                SUM(TRY_CAST(REPLACE("Valor Pago (R$)", ',', '.') AS DOUBLE)) AS valor_pago,
                COUNT(*) AS qtd_lancamentos
            FROM read_csv('{tmp}', delim=';', header=true, encoding='latin-1',
                          ignore_errors=true, quote='"')
            WHERE "UF" IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5, 6, 7
        """
        rows = con.execute(sql).fetchall()
    finally:
        for p in (tmp_zip, tmp_csv):
            if p is None:
                continue
            try:
                p.unlink()
            except FileNotFoundError:
                pass

    for uf, mun, cf, nf, csf, nsf, cp, ve, vl, vp, qtd in rows:
        ibge = uf_slug_to_ibge.get((uf.upper(), _slug(mun))) if uf and mun else None
        yield {
            "mes_competencia":  mes_iso,
            "ibge_code":        ibge,
            "uf":               uf,
            "municipio":        mun,
            "codigo_funcao":    cf,
            "nome_funcao":      nf,
            "codigo_subfuncao": csf,
            "nome_subfuncao":   nsf,
            "codigo_programa":  cp,
            "valor_empenhado":  ve,
            "valor_liquidado":  vl,
            "valor_pago":       vp,
            "qtd_lancamentos":  qtd,
        }


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--month", help="YYYYMM single month")
    add_common_args(p, include_table=False)
    return p.parse_args()


def main() -> int:
    args = _build_args()
    if not args.no_fetch:
        ensure_fetched("orcamento_execucao", refresh=args.refresh)
    uf_slug_to_ibge = _build_uf_slug_map()

    @dlt.resource(
        name="orcamento_execucao_municipal",
        primary_key=["mes_competencia", "ibge_code", "codigo_funcao",
                     "codigo_subfuncao", "codigo_programa"],
        write_disposition="append",
        # Column hints — dlt inferia ibge_code como non-nullable da
        # primeira batch (UF=AC tem ibge), depois falhava em rows com
        # ibge null (municípios sem match no slug). Força nullable + tipo.
        columns={
            "ibge_code":       {"data_type": "bigint", "nullable": True},
            "uf":              {"data_type": "text",   "nullable": True},
            "municipio":       {"data_type": "text",   "nullable": True},
            "codigo_funcao":   {"data_type": "text",   "nullable": True},
            "nome_funcao":     {"data_type": "text",   "nullable": True},
            "codigo_subfuncao":{"data_type": "text",   "nullable": True},
            "nome_subfuncao":  {"data_type": "text",   "nullable": True},
            "codigo_programa": {"data_type": "text",   "nullable": True},
            "valor_empenhado": {"data_type": "double", "nullable": True},
            "valor_liquidado": {"data_type": "double", "nullable": True},
            "valor_pago":      {"data_type": "double", "nullable": True},
            "qtd_lancamentos": {"data_type": "bigint", "nullable": True},
        },
    )
    def orcamento():
        keys = sorted(list_keys(S3_PREFIX))
        for key in keys:
            name = key.rsplit("/", 1)[-1]
            m = FILE_RE.match(name)
            if not m:
                continue
            ym = m.group(1)
            if args.month and ym != args.month:
                continue
            t0 = time.monotonic()
            n = 0
            for rec in _aggregate_month(key, ym, uf_slug_to_ibge):
                n += 1
                yield rec
            print(f"  orcamento {ym}: {n:,} agg em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="orcamento_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(orcamento())
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT count(*), count(ibge_code), sum(valor_pago) "
                                "FROM orcamento_execucao_municipal"))
        return 0

    pipe = dlt.pipeline(pipeline_name="orcamento", destination=s3tables_iceberg)
    info = pipe.run(orcamento())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
