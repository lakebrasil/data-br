"""DataSUS CNES — Cadastro Nacional de Estabelecimentos Saúde → indicadores_serie.

Carrega `s3://data-br-raw/datasus/raw/cnes_{YYYYMM}.zip` (~600 MB),
extrai `tbEstabelecimento{YYYYMM}.csv` (~270 MB), agrega por
(município, TP_UNIDADE) e materializa em `data_br.indicadores_serie`
(fonte='cnes', periodo=YYYY-MM).

Indicadores emitidos (1 por município por tipo de unidade):
  cnes.estab.{tipo_code}     ← count(estabelecimentos do tipo_code)
  cnes.estab_total           ← count(total)
  cnes.estab_publico         ← count NIVEL_DEP em {1,2,3,4} (gestão pública)
  cnes.estab_privado         ← count NIVEL_DEP em {5,6} (privada)

Por que indicadores_serie (não tabela própria): mantém compat sem
CDK deploy. Pra detalhes por-CNES (leitos, equipamentos, profissionais),
fica pipeline futuro `_pipelines/datasus_cnes_detail.py` com schema próprio.

Source: cnes.datasus.gov.br/pages/downloads/arquivosBaseDados.jsp.
6-dígito CO_MUNICIPIO_GESTOR → 7-dígito IBGE via map auxiliar
(ibge_code // 10 == co_mun_6).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.datasus_cnes --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.datasus_cnes --dry-run
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
from lakebrasil.common.incremental import loaded_pairs
from lakebrasil.common.s3 import RAW_BUCKET, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "datasus/raw/"
ZIP_RE = re.compile(r"^cnes_(\d{6})\.zip$")  # cnes_202503.zip

# NIVEL_DEP no CNES: 1-4 = gestão municipal/estadual/federal (público),
# 5-6 = privado / outros.
NIVEL_PUBLICO = {"1", "2", "3", "4", "01", "02", "03", "04"}
NIVEL_PRIVADO = {"5", "6", "05", "06"}


def _build_co6_to_ibge() -> dict[int, int]:
    """6-dígito CO_MUNICIPIO (CNES, IBGE-truncated) → ibge_code 7-dígitos."""
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code",)
    ).to_arrow()
    out: dict[int, int] = {}
    for ibge in arrow.column("ibge_code").to_pylist():
        if ibge is None:
            continue
        out[int(ibge) // 10] = int(ibge)
    return out


def _build_co6_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    out: dict[int, str] = {}
    for ibge, uf in zip(arrow.column("ibge_code").to_pylist(),
                        arrow.column("uf").to_pylist()):
        if ibge is None:
            continue
        out[int(ibge) // 10] = uf
    return out


def _aggregate_estabelecimentos(
    zip_bytes: bytes,
    inner_csv: str,
    co6_to_ibge: dict[int, int],
) -> dict[tuple[int, str], int]:
    """Stream tbEstabelecimento CSV → Counter por (ibge_code, indicador).

    indicador é um de:
      'cnes.estab_total'
      'cnes.estab_publico' / 'cnes.estab_privado'
      'cnes.estab.{tipo_unidade}'  (TP_UNIDADE code)
    """
    counter: Counter[tuple[int, str]] = Counter()
    rows_seen = 0
    rows_skip_ibge = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(inner_csv) as fh:
            text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="",
                                       errors="replace")
            reader = csv.DictReader(text_io, delimiter=";")
            for r in reader:
                rows_seen += 1
                co_mun_raw = (r.get("CO_MUNICIPIO_GESTOR") or "").strip()
                if not co_mun_raw:
                    rows_skip_ibge += 1
                    continue
                try:
                    co6 = int(co_mun_raw)
                except ValueError:
                    rows_skip_ibge += 1
                    continue
                ibge = co6_to_ibge.get(co6)
                if ibge is None:
                    rows_skip_ibge += 1
                    continue
                counter[(ibge, "cnes.estab_total")] += 1
                nivel_dep = (r.get("NIVEL_DEP") or "").strip()
                if nivel_dep in NIVEL_PUBLICO:
                    counter[(ibge, "cnes.estab_publico")] += 1
                elif nivel_dep in NIVEL_PRIVADO:
                    counter[(ibge, "cnes.estab_privado")] += 1
                tipo = (r.get("TP_UNIDADE") or "").strip()
                if tipo:
                    counter[(ibge, f"cnes.estab.tipo_{tipo}")] += 1
                if rows_seen % 100_000 == 0:
                    print(f"  CNES estab: {rows_seen:,} rows, "
                          f"{len(counter):,} (ibge,indicador) chaves",
                          file=sys.stderr)
    print(f"  CNES estab: total={rows_seen:,} skip_ibge={rows_skip_ibge:,} "
          f"chaves={len(counter):,}", file=sys.stderr)
    return counter


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("cnes_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/datasus/raw/cnes_*.zip já populado.",
                  file=sys.stderr)

    co6_to_ibge = _build_co6_to_ibge()
    co6_to_uf = _build_co6_to_uf()
    print(f"CNES→IBGE índice: {len(co6_to_ibge):,} municípios", file=sys.stderr)

    already = set() if args.full_refresh else loaded_pairs(
        "indicadores_serie", "fonte", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def cnes_indicadores() -> Iterator[dict]:
        keys = sorted(list_keys(S3_PREFIX))
        for key in keys:
            name = key.rsplit("/", 1)[-1]
            m = ZIP_RE.match(name)
            if not m:
                continue
            yyyymm = m.group(1)  # 202503
            periodo = f"{yyyymm[:4]}-{yyyymm[4:]}"
            if ("cnes", periodo) in already:
                continue
            t0 = time.monotonic()
            print(f"  CNES download {key}", file=sys.stderr)
            body = boto3.client("s3").get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
            inner = f"tbEstabelecimento{yyyymm}.csv"
            counter = _aggregate_estabelecimentos(body, inner, co6_to_ibge)
            for (ibge, indicador), qtd in counter.items():
                # uf via reverse lookup
                co6 = ibge // 10
                uf = co6_to_uf.get(co6, "??")
                yield {
                    "ibge_code":     ibge,
                    "uf":            uf,
                    "fonte":         "cnes",
                    "indicador_id":  indicador,
                    "periodo":       periodo,
                    "valor":         float(qtd),
                    "valor_texto":   None,
                    "unidade":       "estabelecimentos",
                    "fonte_arquivo": name,
                }
            print(f"  CNES {periodo}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="cnes_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(cnes_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), sum(valor) FROM indicadores_serie "
                "GROUP BY indicador_id ORDER BY count(*) DESC LIMIT 15"))
        return 0

    pipe = dlt.pipeline(pipeline_name="datasus_cnes", destination=s3tables_iceberg)
    info = pipe.run(cnes_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
