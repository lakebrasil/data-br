"""FNDE — Fundo Nacional de Desenvolvimento da Educação → indicadores_serie.

Lê arquivos em `s3://data-br-raw/mec/raw/fnde_*/`:

  fnde_fundeb/FUNDEB_REPASSES.csv          # 1 row/mês/município, repasses Fundeb
  fnde_siope/DADOS_CONSOLIDADOS_2019.csv   # receitas+despesas educacionais anuais
  fnde_pdde/PDDE_ESTIMATIVAS_*.csv         # estimativas PDDE por escola (deixar pra futuro)
  fnde_pnae/PNAE_ALUNOS_ATENDIDOS_*.csv    # alunos atendidos PNAE (deixar pra futuro)

V1 cobre Fundeb + SIOPE consolidados (~9 indicadores).

Fundeb REPASSES (col ENT_GOVERNAMENTAL = nome município; precisa
resolve_ibge):
  fnde.fundeb_repasse_ano   sum(VL_REPASSE)   R$
  fnde.fundeb_alunos        max(QTD_ALUNOS)   (alunos atendidos)

SIOPE DADOS_CONSOLIDADOS (TP_PERIODO=ANUAL, CO_MUNICIPIO_IBGE direto):
  fnde.siope_receita_realizada       VL_RECEITA_REALIZADA
  fnde.siope_despesa_empenhada       VL_DESPESA_EMPENHADA
  fnde.siope_despesa_liquidada       VL_DESPESA_LIQUIDADA
  fnde.siope_despesa_paga            VL_DESPESA_PAGA
  fnde.siope_despesa_educ_empenhada  VL_DESPESA_EMPENHADA_EDUCACAO
  fnde.siope_despesa_educ_liquidada  VL_DESPESA_LIQUIDADA_EDUCACAO
  fnde.siope_despesa_educ_paga       VL_DESPESA_PAGA_EDUCACAO

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.fnde --no-fetch
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
from collections import defaultdict
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import municipios_count, resolve_ibge
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_FUNDEB = "mec/raw/fnde_fundeb/FUNDEB_REPASSES.csv"
S3_SIOPE  = "mec/raw/fnde_siope/DADOS_CONSOLIDADOS_2019.csv"


def _build_ibge_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    return {int(i): u or "??" for i, u in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
    ) if i is not None}


def _parse_money_br(s: str | None) -> float:
    if not s:
        return 0.0
    s = s.strip().replace(".", "").replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _iter_fundeb(ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """Stream FUNDEB_REPASSES.csv → 1 row por (município, ano) agregado.

    Schema: ANO;UF;ENT_GOVERNAMENTAL(nome);MES_RECEITA;VL_REPASSE;QTD_ALUNOS
    """
    t0 = time.monotonic()
    raw = get_object_bytes(S3_FUNDEB)
    print(f"  FNDE Fundeb: {len(raw)/1e6:.1f} MB raw", file=sys.stderr)
    text = io.TextIOWrapper(io.BytesIO(raw), encoding="latin-1",
                            newline="", errors="replace")
    reader = csv.reader(text, delimiter=";")
    header = next(reader, None)
    if not header:
        return
    # VL_REPASSE é CUMULATIVO ao longo do ano (mês 12 = total do ano).
    # accum[(ibge, ano)] = (max_repasse, max_alunos)
    accum: dict[tuple[int, str], list[float]] = defaultdict(
        lambda: [0.0, 0.0])
    rows_seen = 0
    skip_ibge = 0
    for row in reader:
        rows_seen += 1
        if len(row) < 6:
            continue
        ano = row[0].strip()
        uf = row[1].strip().upper()
        mun = row[2].strip()
        if not (ano.isdigit() and uf and mun):
            continue
        ibge = resolve_ibge(uf, mun)
        if ibge is None:
            skip_ibge += 1
            continue
        vl = _parse_money_br(row[4])
        qt = _parse_money_br(row[5])
        k = (ibge, ano)
        accum[k][0] = max(accum[k][0], vl)
        accum[k][1] = max(accum[k][1], qt)
    print(f"  FNDE Fundeb: {rows_seen:,} rows, skip_ibge={skip_ibge:,} → "
          f"{len(accum):,} (mun, ano) em {time.monotonic()-t0:.1f}s",
          file=sys.stderr)
    for (ibge, ano), (sum_vl, max_qt) in accum.items():
        uf = ibge_to_uf.get(ibge, "??")
        yield {
            "ibge_code": ibge, "uf": uf,
            "fonte": "fnde",
            "indicador_id": "fnde.fundeb_repasse_ano",
            "periodo": ano, "valor": sum_vl,
            "valor_texto": None, "unidade": "R$",
            "fonte_arquivo": "FUNDEB_REPASSES.csv",
        }
        if max_qt > 0:
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "fnde",
                "indicador_id": "fnde.fundeb_alunos",
                "periodo": ano, "valor": max_qt,
                "valor_texto": None, "unidade": "alunos",
                "fonte_arquivo": "FUNDEB_REPASSES.csv",
            }


SIOPE_COLS = [
    ("VL_RECEITA_REALIZADA",         "siope_receita_realizada"),
    ("VL_DESPESA_EMPENHADA",         "siope_despesa_empenhada"),
    ("VL_DESPESA_LIQUIDADA",         "siope_despesa_liquidada"),
    ("VL_DESPESA_PAGA",              "siope_despesa_paga"),
    ("VL_DESPESA_EMPENHADA_EDUCACAO", "siope_despesa_educ_empenhada"),
    ("VL_DESPESA_LIQUIDADA_EDUCACAO", "siope_despesa_educ_liquidada"),
    ("VL_DESPESA_PAGA_EDUCACAO",     "siope_despesa_educ_paga"),
]


def _iter_siope(ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """Stream SIOPE DADOS_CONSOLIDADOS → 1 row por (município, ano)
    filtrando TP_PERIODO=ANUAL (snapshot consolidado do exercício).

    SIOPE chama a coluna `CO_MUNICIPIO_IBGE` mas o valor é IBGE TRUNCADO
    6-dig (sem o check digit). Igual SNIS. Converte via co6_to_ibge."""
    # Constrói índice 6-dig → 7-dig
    co6_to_ibge = {(i // 10): i for i in ibge_to_uf}
    t0 = time.monotonic()
    raw = get_object_bytes(S3_SIOPE)
    print(f"  FNDE SIOPE: {len(raw)/1e6:.1f} MB raw", file=sys.stderr)
    text = io.TextIOWrapper(io.BytesIO(raw), encoding="latin-1",
                            newline="", errors="replace")
    reader = csv.DictReader(text, delimiter=";")
    rows_seen = 0
    rows_filtered = 0
    munis_seen: set[int] = set()
    for r in reader:
        rows_seen += 1
        if (r.get("TP_PERIODO") or "").strip().upper() != "ANUAL":
            continue
        cm = (r.get("CO_MUNICIPIO_IBGE") or "").strip()
        if not cm or not cm.isdigit():
            continue
        co6 = int(cm)
        ibge = co6_to_ibge.get(co6)
        if ibge is None:
            continue
        ano = (r.get("#AN_EXERCICIO") or r.get("AN_EXERCICIO") or "").strip()
        if not ano.isdigit():
            continue
        rows_filtered += 1
        munis_seen.add(ibge)
        uf = ibge_to_uf.get(ibge, "??")
        for col, suffix in SIOPE_COLS:
            v = _parse_money_br(r.get(col))
            if v == 0:
                continue
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "fnde",
                "indicador_id": f"fnde.{suffix}",
                "periodo": ano, "valor": v,
                "valor_texto": None, "unidade": "R$",
                "fonte_arquivo": "DADOS_CONSOLIDADOS_2019.csv",
            }
    print(f"  FNDE SIOPE: {rows_seen:,} rows → {rows_filtered:,} ANUAL "
          f"em {len(munis_seen):,} munis ({time.monotonic()-t0:.1f}s)",
          file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--source", action="append",
                   choices=["fundeb", "siope"],
                   help="Subset: fundeb / siope (default: ambos)")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("fnde_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://.../mec/raw/fnde_*/ já populado.",
                  file=sys.stderr)

    municipios_count()
    ibge_to_uf = _build_ibge_to_uf()
    sources = set(args.source) if args.source else {"fundeb", "siope"}

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="fnde")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def fnde_indicadores() -> Iterator[dict]:
        if "fundeb" in sources:
            for rec in _iter_fundeb(ibge_to_uf):
                if ("fnde", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec
        if "siope" in sources:
            for rec in _iter_siope(ibge_to_uf):
                if ("fnde", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="fnde_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(fnde_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, periodo, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id, periodo "
                "ORDER BY periodo, indicador_id LIMIT 30"))
        return 0

    pipe = dlt.pipeline(pipeline_name="fnde", destination=s3tables_iceberg)
    info = pipe.run(fnde_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
