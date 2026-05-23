"""MEC Censo da Educação Superior → indicadores_serie.

Lê `s3://data-br-raw/mec/raw/microdados_censo_da_educacao_superior_{ano}.zip`
e processa 2 CSVs por ano:

  MICRODADOS_ED_SUP_IES.CSV       (~1MB, ~2.500 IES Brasil, 1 row/IES)
    → cols CO_MUNICIPIO_IES, TP_CATEGORIA_ADMINISTRATIVA, QT_DOC_TOTAL,
            QT_TEC_TOTAL, TP_ORGANIZACAO_ACADEMICA, ...

  MICRODADOS_CADASTRO_CURSOS.CSV  (~390MB, ~40k cursos, 1 row/curso)
    → cols CO_MUNICIPIO, TP_GRAU_ACADEMICO, QT_MAT, QT_ING, QT_CONC, ...

Agrega por município → 13 indicadores:

  IES counts (1 row/muni):
    mec_es.ies_total
    mec_es.ies_federal / estadual / municipal / privada   (TP_CATEGORIA_ADMINISTRATIVA)
    mec_es.ies_universidade                               (TP_ORGANIZACAO_ACADEMICA=1)
    mec_es.docentes_ies_total                             (sum QT_DOC_TOTAL)
    mec_es.tecnicos_ies_total                             (sum QT_TEC_TOTAL)

  Cursos sums (por município onde o curso é OFERECIDO):
    mec_es.cursos_total
    mec_es.matriculados_total      (sum QT_MAT)
    mec_es.ingressantes_total      (sum QT_ING)
    mec_es.concluintes_total       (sum QT_CONC)

CO_MUNICIPIO* = IBGE 7-dig direto. Periodo = NU_ANO_CENSO.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.mec_ed_superior --no-fetch
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
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "mec/raw/"
ZIP_RE = re.compile(r"^microdados_censo_da_educacao_superior_(\d{4})\.zip$")

IES_RE = re.compile(r"MICRODADOS_ED_SUP_IES_\d{4}\.csv$", re.IGNORECASE)
CURSOS_RE = re.compile(r"MICRODADOS_CADASTRO_CURSOS_\d{4}\.csv$", re.IGNORECASE)


def _build_ibge_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    return {int(i): u or "??" for i, u in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
    ) if i is not None}


def _parse_int(v: str) -> int:
    try:
        return int(float((v or "").strip() or "0"))
    except ValueError:
        return 0


def _iter_ano(zip_bytes: bytes, ano: int,
              ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """Processa 1 ano (ZIP) — emite indicadores por município."""
    t0 = time.monotonic()
    periodo = str(ano)

    # 1) IES file (~1MB)
    ies_counts: dict[int, dict[str, int]] = defaultdict(
        lambda: defaultdict(int))
    ies_sums: dict[int, dict[str, int]] = defaultdict(
        lambda: defaultdict(int))

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        ies_name = next((n for n in zf.namelist() if IES_RE.search(n)), None)
        cursos_name = next((n for n in zf.namelist() if CURSOS_RE.search(n)),
                           None)
        if not ies_name or not cursos_name:
            print(f"  MEC-ES {ano}: arquivos faltando "
                  f"(ies={ies_name} cursos={cursos_name})", file=sys.stderr)
            return

        # IES
        with zf.open(ies_name) as fh:
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="",
                                    errors="replace")
            reader = csv.DictReader(text, delimiter=";")
            for r in reader:
                cm = (r.get("CO_MUNICIPIO_IES") or "").strip()
                if not cm or not cm.isdigit():
                    continue
                ibge = int(cm)
                if ibge not in ibge_to_uf:
                    continue
                ies_counts[ibge]["total"] += 1
                cat = (r.get("TP_CATEGORIA_ADMINISTRATIVA") or "").strip()
                if cat == "1":
                    ies_counts[ibge]["federal"] += 1
                elif cat == "2":
                    ies_counts[ibge]["estadual"] += 1
                elif cat == "3":
                    ies_counts[ibge]["municipal"] += 1
                elif cat == "4":
                    ies_counts[ibge]["privada"] += 1
                org = (r.get("TP_ORGANIZACAO_ACADEMICA") or "").strip()
                if org == "1":
                    ies_counts[ibge]["universidade"] += 1
                ies_sums[ibge]["docentes"] += _parse_int(r.get("QT_DOC_TOTAL"))
                ies_sums[ibge]["tecnicos"] += _parse_int(r.get("QT_TEC_TOTAL"))
        print(f"  MEC-ES {ano} IES: {sum(c['total'] for c in ies_counts.values()):,} "
              f"IES em {len(ies_counts):,} munis", file=sys.stderr)

        # CURSOS (390MB, stream-parse)
        cursos_sums: dict[int, dict[str, int]] = defaultdict(
            lambda: defaultdict(int))
        with zf.open(cursos_name) as fh:
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="",
                                    errors="replace")
            reader = csv.DictReader(text, delimiter=";")
            n_cursos = 0
            for r in reader:
                cm = (r.get("CO_MUNICIPIO") or "").strip()
                if not cm or not cm.isdigit():
                    continue
                ibge = int(cm)
                if ibge not in ibge_to_uf:
                    continue
                cursos_sums[ibge]["cursos"] += 1
                cursos_sums[ibge]["matriculados"] += _parse_int(r.get("QT_MAT"))
                cursos_sums[ibge]["ingressantes"] += _parse_int(r.get("QT_ING"))
                cursos_sums[ibge]["concluintes"] += _parse_int(r.get("QT_CONC"))
                n_cursos += 1
            print(f"  MEC-ES {ano} CURSOS: {n_cursos:,} cursos em "
                  f"{len(cursos_sums):,} munis", file=sys.stderr)

    # Emit IES indicators
    fonte_arq = f"microdados_censo_da_educacao_superior_{ano}.zip"
    emitted = 0
    all_munis = set(ies_counts) | set(cursos_sums)
    for ibge in all_munis:
        uf = ibge_to_uf.get(ibge, "??")
        # IES counts
        for label, val in ies_counts.get(ibge, {}).items():
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "mec_es",
                "indicador_id": f"mec_es.ies_{label}",
                "periodo": periodo, "valor": float(val),
                "valor_texto": None, "unidade": "instituições",
                "fonte_arquivo": fonte_arq,
            }
            emitted += 1
        # IES sums (docentes, técnicos)
        for label, val in ies_sums.get(ibge, {}).items():
            if val == 0:
                continue
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "mec_es",
                "indicador_id": f"mec_es.{label}_ies_total",
                "periodo": periodo, "valor": float(val),
                "valor_texto": None,
                "unidade": "docentes" if label == "docentes" else "técnicos",
                "fonte_arquivo": fonte_arq,
            }
            emitted += 1
        # Cursos sums
        for label, val in cursos_sums.get(ibge, {}).items():
            if val == 0:
                continue
            unidade = ("cursos" if label == "cursos"
                       else ("matrículas" if label == "matriculados"
                             else "alunos"))
            yield {
                "ibge_code": ibge, "uf": uf,
                "fonte": "mec_es",
                "indicador_id": f"mec_es.{label}_total",
                "periodo": periodo, "valor": float(val),
                "valor_texto": None, "unidade": unidade,
                "fonte_arquivo": fonte_arq,
            }
            emitted += 1
    print(f"  MEC-ES {ano}: {emitted:,} rows em {len(all_munis):,} munis "
          f"({time.monotonic()-t0:.1f}s)", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ano", type=int, action="append",
                   help="Anos a carregar (default: todos no raw).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("mec_ed_superior_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://.../mec/raw/microdados_censo_da_educacao_superior_*.zip já populado.",
                  file=sys.stderr)

    ibge_to_uf = _build_ibge_to_uf()
    print(f"MEC-ES→ibge dim: {len(ibge_to_uf):,} munis válidos", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="mec_es")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def mec_es_indicadores() -> Iterator[dict]:
        anos_filter = set(args.ano) if args.ano else None
        for key in sorted(list_keys(S3_PREFIX)):
            name = key.rsplit("/", 1)[-1]
            m = ZIP_RE.match(name)
            if not m:
                continue
            ano = int(m.group(1))
            if anos_filter and ano not in anos_filter:
                continue
            print(f"  MEC-ES {name}: download", file=sys.stderr)
            t0 = time.monotonic()
            zip_bytes = get_object_bytes(key)
            print(f"  MEC-ES {name}: {len(zip_bytes)/1e6:.0f} MB em "
                  f"{time.monotonic()-t0:.1f}s", file=sys.stderr)
            for rec in _iter_ano(zip_bytes, ano, ibge_to_uf):
                if ("mec_es", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="mec_es_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(mec_es_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), sum(valor) "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC LIMIT 20"))
        return 0

    pipe = dlt.pipeline(pipeline_name="mec_ed_superior",
                        destination=s3tables_iceberg)
    info = pipe.run(mec_es_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
