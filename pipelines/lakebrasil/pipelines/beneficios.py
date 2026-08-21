"""BPC + Bolsa Família agregados a nível municipal → data_br.beneficios_municipais.

Raw: CSV mensal latin-1 (BPC ~1.2 GB descompactado, BF ~2.2 GB), CPF-level.
Para o datalake municipal, agregamos via DuckDB (que lê zip + CSV
nativamente) por (programa, mes_competencia, ibge_code) →
SUM(valor_parcela), COUNT(distinct cpf), COUNT(parcelas).

`MES COMPETENCIA` no raw vem como '202503' (YYYYMM); convertemos pra
'YYYY-MM' para alinhar com o resto do datalake.
`CODIGO MUNICIPIO SIAFI` é código SIAFI, não IBGE — junta via
municipios.codigo_siafi (que veio do municipios-br seed) → ibge_code.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.beneficios --programa BPC --month 202503 --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.beneficios --programa BPC
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.beneficios   # ambos
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import dlt
import duckdb

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import siafi_to_ibge_map
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_pairs
from lakebrasil.common.s3 import RAW_BUCKET, list_keys, local_path, s3_client
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

PROGRAMS = {
    "BPC": {
        "s3_prefix": "bpc/raw/",
        "file_re": re.compile(r"^(\d{6})_BPC\.zip$"),
        "csv_re":  re.compile(r"^(\d{6})_BPC\.csv$"),
        # CSV columns we care about (latin-1, semi-colon delim, quoted).
        "value_col": "VALOR PARCELA",
        "cpf_col":   "CPF BENEFICIÁRIO",
    },
    "BOLSA_FAMILIA": {
        "s3_prefix": "bolsa_familia/raw/",
        "file_re": re.compile(r"^(\d{6})_BF\.zip$"),
        # Programa renomeado de Auxílio Brasil → Novo Bolsa Família em
        # mar/2023 (PMV nº 1.155). Zips de 202111 a 202302 contêm
        # `*_AuxilioBrasil.csv`; 202303+ usam `*_NovoBolsaFamilia.csv`.
        # Headers internos são idênticos entre os dois → SQL não muda.
        "csv_re":  re.compile(r"^(\d{6})_(NovoBolsaFamilia|AuxilioBrasil)\.csv$"),
        "value_col": "VALOR PARCELA",
        "cpf_col":   "CPF FAVORECIDO",
    },
}


def _months_already_loaded(programa: str) -> set[str]:
    """Mes já materializado em data_br.beneficios_municipais pra esse
    programa. Reusa loaded_pairs e filtra in-Python (Iceberg row_filter
    via pyiceberg também serviria, mas loaded_pairs é o helper
    padrão)."""
    return {mes for prog, mes in loaded_pairs(
        "beneficios_municipais", "programa", "mes_competencia"
    ) if prog == programa}


def _aggregate_month(programa: str, s3_key: str, mes_yyyymm: str,
                     siafi_map: dict[str, int]) -> Iterator[dict]:
    """zip → DuckDB sql → agregados por município.

    Em mount mode (Fargate + S3 Files): zipfile abre direto do mount,
    extrai CSV pra /tmp (DuckDB precisa file path; zipfile.open dá um
    file-like que DuckDB read_csv não aceita), processa, limpa só o CSV.
    Em fallback (sem mount): baixa zip via boto3 pra /tmp.

    Pico /tmp por mês: ~1 GB (só o CSV; zip lido in-place se mounted).
    """
    cfg = PROGRAMS[programa]
    import zipfile

    mount_zip = local_path(s3_key)
    tmp_zip: Path | None = None
    tmp_csv: Path | None = None
    try:
        # Resolve o zip: mount mode = path direto; senão download.
        if mount_zip is not None:
            zip_to_read = mount_zip
        else:
            tmp_zip = Path("/tmp") / Path(s3_key).name
            s3_client().download_file(RAW_BUCKET, s3_key, str(tmp_zip))
            zip_to_read = tmp_zip

        with zipfile.ZipFile(zip_to_read) as zf:
            csv_inner = next(
                (n for n in zf.namelist() if cfg["csv_re"].match(n)), None
            )
            if csv_inner is None:
                return
            zf.extract(csv_inner, "/tmp")
        tmp_csv = Path("/tmp") / csv_inner
        if tmp_zip is not None:
            try:
                tmp_zip.unlink()
                tmp_zip = None
            except FileNotFoundError:
                pass

        mes_iso = f"{mes_yyyymm[:4]}-{mes_yyyymm[4:6]}"
        con = duckdb.connect()
        sql = f"""
            SELECT
                "UF" AS uf,
                "NOME MUNICÍPIO" AS municipio,
                "CÓDIGO MUNICÍPIO SIAFI" AS siafi,
                SUM(TRY_CAST(REPLACE("{cfg['value_col']}", ',', '.') AS DOUBLE)) AS valor_total,
                COUNT(DISTINCT "{cfg['cpf_col']}") AS qtd_beneficiarios,
                COUNT(*) AS qtd_parcelas
            FROM read_csv('{tmp_csv}', delim=';', header=true, encoding='latin-1',
                          ignore_errors=true, quote='"')
            WHERE "UF" IS NOT NULL
            GROUP BY 1, 2, 3
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

    for uf, municipio, siafi, valor, qtd_b, qtd_p in rows:
        siafi_str = str(siafi).zfill(4) if siafi is not None else None
        ibge = siafi_map.get(siafi_str) if siafi_str else None
        yield {
            "programa":          programa,
            "mes_competencia":   mes_iso,
            "ibge_code":         ibge,
            "uf":                uf,
            "municipio":         municipio,
            "valor_total":       valor,
            "qtd_beneficiarios": qtd_b,
            "qtd_parcelas":      qtd_p,
        }


def _build_resource(programa: str, only_month: str | None = None,
                    full_refresh: bool = False) -> Iterator[dict]:
    cfg = PROGRAMS[programa]
    siafi_map = siafi_to_ibge_map()
    keys = sorted(list_keys(cfg["s3_prefix"]))
    already = set() if (full_refresh or only_month) else _months_already_loaded(programa)
    if already:
        print(f"  {programa}: {len(already)} meses já em Iceberg — pulando",
              file=sys.stderr)
    skipped = 0
    for key in keys:
        name = key.rsplit("/", 1)[-1]
        m = cfg["file_re"].match(name)
        if not m:
            continue
        ym = m.group(1)
        mes_iso = f"{ym[:4]}-{ym[4:6]}"
        if only_month and ym != only_month:
            continue
        if mes_iso in already:
            skipped += 1
            continue
        t0 = time.monotonic()
        n = 0
        for rec in _aggregate_month(programa, key, ym, siafi_map):
            n += 1
            yield rec
        print(f"  {programa} {ym}: {n:,} municípios em {time.monotonic()-t0:.1f}s",
              file=sys.stderr)
    if skipped:
        print(f"  {programa}: {skipped} meses pulados (já carregados)",
              file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--programa", choices=("BPC", "BOLSA_FAMILIA"),
                   help="Default: ambos.")
    p.add_argument("--month", help="YYYYMM single month (debug).")
    add_common_args(p, include_table=False)
    return p.parse_args()


def main() -> int:
    args = _build_args()
    programs = [args.programa] if args.programa else list(PROGRAMS)

    if not args.no_fetch:
        # bpc + bolsa_familia são entradas literais no catalog.yaml.
        cat_names = []
        if "BPC" in programs:
            cat_names.append("bpc")
        if "BOLSA_FAMILIA" in programs:
            cat_names.append("bolsa_familia")
        ensure_fetched(cat_names, refresh=args.refresh)

    @dlt.resource(
        name="beneficios_municipais",
        # write_disposition="merge" com custom destination + 488K rows
        # estava dropando ~16 meses iniciais de BF silenciosamente. Usamos
        # append + filtro incremental no resource (`_months_already_loaded`)
        # — cada run carrega só o delta. --full-refresh força tudo (use
        # depois de DROP TABLE pra reload limpo, senão vai duplicar).
        write_disposition="append",
    )
    def beneficios():
        for prog in programs:
            yield from _build_resource(
                prog, only_month=args.month, full_refresh=args.full_refresh,
            )

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="beneficios_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(beneficios())
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT programa, count(*), count(ibge_code), "
                                "sum(valor_total), sum(qtd_beneficiarios) "
                                "FROM beneficios_municipais GROUP BY programa"))
        return 0

    pipe = dlt.pipeline(pipeline_name="beneficios", destination=s3tables_iceberg)
    info = pipe.run(beneficios())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
