"""INEP Censo Escolar → data_br.indicadores_serie.

Lê `s3://data-br-raw/inep/raw/censo_escolar_{ano}.zip` (microdados
educação básica, 1 row por escola, ~180k escolas Brasil) e agrega por
município (CO_MUNICIPIO 7-dig IBGE).

Emite ~28 indicadores por município:

  Contagens de escolas (uma linha por bucket):
    inep.escolas_total
    inep.escolas_federal / estadual / municipal / privada      (TP_DEPENDENCIA)
    inep.escolas_urbana / rural                                 (TP_LOCALIZACAO)

  Matrículas totais (soma de QT_MAT_*):
    inep.matriculas_total          QT_MAT_BAS
    inep.matriculas_infantil       QT_MAT_INF (creche + pré)
    inep.matriculas_fundamental    QT_MAT_FUND
    inep.matriculas_medio          QT_MAT_MED

  % escolas com infraestrutura (mean(IN_*) × 100):
    inep.pct_escolas_internet           IN_INTERNET
    inep.pct_escolas_biblioteca         IN_BIBLIOTECA
    inep.pct_escolas_lab_informatica    IN_LABORATORIO_INFORMATICA
    inep.pct_escolas_lab_ciencias       IN_LABORATORIO_CIENCIAS
    inep.pct_escolas_quadra_esportes    IN_QUADRA_ESPORTES
    inep.pct_escolas_refeitorio         IN_REFEITORIO
    inep.pct_escolas_computador         IN_COMPUTADOR
    inep.pct_escolas_acessibilidade     IN_ACESSIBILIDADE_RAMPAS

  % escolas com saneamento:
    inep.pct_escolas_agua_rede          IN_AGUA_REDE_PUBLICA
    inep.pct_escolas_energia_rede       IN_ENERGIA_REDE_PUBLICA
    inep.pct_escolas_esgoto_rede        IN_ESGOTO_REDE_PUBLICA
    inep.pct_escolas_lixo_coleta        IN_LIXO_SERVICO_COLETA

  Razão alunos/docente (sum_mat / sum_doc):
    inep.razao_aluno_docente            QT_MAT_BAS / QT_DOC_BAS

Periodo = NU_ANO_CENSO (ano do censo, ex '2024').

CSV é ~218MB/ano, latin-1 encoding (gov.br padrão). Streaming via csv.reader
+ defaultdict por município — sem pandas pra evitar 1GB+ RAM.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.inep --no-fetch
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
from lakebrasil.common.s3 import RAW_BUCKET, get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "inep/raw/"
ZIP_RE = re.compile(r"^censo_escolar_(\d{4})\.zip$")

# (col_name, indicador_suffix, mode)
# mode='count' → 1 row aggregator counts escolas com cond. (TP_X == val)
# mode='sum'   → soma de QT_X
# mode='mean'  → média de IN_X (% escolas com flag)
# mode='ratio' → razao a/b

CONTAGEM_FILTERS = [
    # (filter_col, filter_val, indicador_suffix)
    ("TP_DEPENDENCIA", "1", "federal"),
    ("TP_DEPENDENCIA", "2", "estadual"),
    ("TP_DEPENDENCIA", "3", "municipal"),
    ("TP_DEPENDENCIA", "4", "privada"),
    ("TP_LOCALIZACAO", "1", "urbana"),
    ("TP_LOCALIZACAO", "2", "rural"),
]

SUM_COLS = [
    ("QT_MAT_BAS",  "matriculas_total"),
    ("QT_MAT_INF",  "matriculas_infantil"),
    ("QT_MAT_FUND", "matriculas_fundamental"),
    ("QT_MAT_MED",  "matriculas_medio"),
    ("QT_DOC_BAS",  "docentes_total"),
]

MEAN_COLS = [
    ("IN_INTERNET",                "pct_escolas_internet"),
    ("IN_BIBLIOTECA",              "pct_escolas_biblioteca"),
    ("IN_LABORATORIO_INFORMATICA", "pct_escolas_lab_informatica"),
    ("IN_LABORATORIO_CIENCIAS",    "pct_escolas_lab_ciencias"),
    ("IN_QUADRA_ESPORTES",         "pct_escolas_quadra_esportes"),
    ("IN_REFEITORIO",              "pct_escolas_refeitorio"),
    ("IN_COMPUTADOR",              "pct_escolas_computador"),
    ("IN_ACESSIBILIDADE_RAMPAS",   "pct_escolas_acessibilidade_rampas"),
    ("IN_AGUA_REDE_PUBLICA",       "pct_escolas_agua_rede"),
    ("IN_ENERGIA_REDE_PUBLICA",    "pct_escolas_energia_rede"),
    ("IN_ESGOTO_REDE_PUBLICA",     "pct_escolas_esgoto_rede"),
    ("IN_LIXO_SERVICO_COLETA",     "pct_escolas_lixo_coleta"),
]


def _build_ibge_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    out: dict[int, str] = {}
    for ibge, uf in zip(arrow.column("ibge_code").to_pylist(),
                        arrow.column("uf").to_pylist()):
        if ibge is not None:
            out[int(ibge)] = uf or "??"
    return out


def _iter_censo(zip_bytes: bytes, ano: int,
                ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    """Stream-aggregate microdados → 1 row por (município, indicador)."""
    target_re = re.compile(r"microdados_ed_basica_\d{4}\.csv$", re.IGNORECASE)
    csv_name = None
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for n in zf.namelist():
            if target_re.search(n):
                csv_name = n
                break
        if not csv_name:
            print(f"  INEP {ano}: microdados_ed_basica_*.csv não encontrado",
                  file=sys.stderr)
            return
        print(f"  INEP {ano}: lendo {csv_name}", file=sys.stderr)
        with zf.open(csv_name) as fh:
            # Latin-1 é o encoding padrão dos microdados INEP. errors=replace
            # protege contra bytes órfãos em nomes de escola.
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="",
                                    errors="replace")
            reader = csv.DictReader(text, delimiter=";")
            # Accum por município:
            #   counts[mun] = {bucket: int}  (escolas_total, escolas_federal, etc.)
            #   sums[mun]   = {col: float}   (matrículas totais)
            #   means[mun]  = {col: (sum, n)} (% escolas com flag)
            counts: dict[int, dict[str, int]] = defaultdict(
                lambda: defaultdict(int))
            sums: dict[int, dict[str, float]] = defaultdict(
                lambda: defaultdict(float))
            means: dict[int, dict[str, list[int]]] = defaultdict(
                lambda: defaultdict(lambda: [0, 0]))
            rows_seen = 0
            rows_skip = 0
            for r in reader:
                rows_seen += 1
                co_mun = r.get("CO_MUNICIPIO", "").strip()
                if not co_mun or not co_mun.isdigit():
                    rows_skip += 1
                    continue
                ibge = int(co_mun)
                if ibge not in ibge_to_uf:
                    rows_skip += 1
                    continue
                # 1 escola = 1 row. Total cont:
                counts[ibge]["total"] += 1
                # Buckets condicionais
                for fcol, fval, suffix in CONTAGEM_FILTERS:
                    if (r.get(fcol) or "").strip() == fval:
                        counts[ibge][suffix] += 1
                # Sums (matrículas / docentes)
                for col, suffix in SUM_COLS:
                    v = (r.get(col) or "").strip()
                    if not v:
                        continue
                    try:
                        sums[ibge][suffix] += float(v)
                    except ValueError:
                        pass
                # Mean (IN_ flags) — só considera escolas em funcionamento
                # (TP_SITUACAO_FUNCIONAMENTO=1). Sem essa filtro algumas IN_
                # vêm vazias e contam como 0 falsamente.
                situ = (r.get("TP_SITUACAO_FUNCIONAMENTO") or "").strip()
                if situ == "1":
                    for col, suffix in MEAN_COLS:
                        v = (r.get(col) or "").strip()
                        if v in ("0", "1"):
                            means[ibge][suffix][0] += int(v)
                            means[ibge][suffix][1] += 1
            print(f"  INEP {ano}: {rows_seen:,} escolas, skip={rows_skip:,} → "
                  f"{len(counts):,} munis", file=sys.stderr)

    periodo = str(ano)
    emitted = 0
    for ibge, c in counts.items():
        uf = ibge_to_uf.get(ibge, "??")
        # Contagens
        for label, val in c.items():
            yield {
                "ibge_code":     ibge,
                "uf":            uf,
                "fonte":         "inep",
                "indicador_id":  f"inep.escolas_{label}",
                "periodo":       periodo,
                "valor":         float(val),
                "valor_texto":   None,
                "unidade":       "escolas",
                "fonte_arquivo": f"censo_escolar_{ano}.zip",
            }
            emitted += 1
        # Sums (matrículas + docentes)
        for col, suffix in SUM_COLS:
            v = sums.get(ibge, {}).get(suffix, 0.0)
            if v == 0.0:
                continue
            yield {
                "ibge_code":     ibge,
                "uf":            uf,
                "fonte":         "inep",
                "indicador_id":  f"inep.{suffix}",
                "periodo":       periodo,
                "valor":         v,
                "valor_texto":   None,
                "unidade":       ("docentes" if "docent" in suffix
                                  else "matriculas"),
                "fonte_arquivo": f"censo_escolar_{ano}.zip",
            }
            emitted += 1
        # Razão aluno/docente (matriculas_total / docentes_total)
        mat = sums.get(ibge, {}).get("matriculas_total", 0.0)
        doc = sums.get(ibge, {}).get("docentes_total", 0.0)
        if doc > 0:
            yield {
                "ibge_code":     ibge,
                "uf":            uf,
                "fonte":         "inep",
                "indicador_id":  "inep.razao_aluno_docente",
                "periodo":       periodo,
                "valor":         mat / doc,
                "valor_texto":   None,
                "unidade":       "alunos/docente",
                "fonte_arquivo": f"censo_escolar_{ano}.zip",
            }
            emitted += 1
        # Means (% escolas com flag)
        for col, suffix in MEAN_COLS:
            soma, n = means.get(ibge, {}).get(suffix, [0, 0])
            if n == 0:
                continue
            yield {
                "ibge_code":     ibge,
                "uf":            uf,
                "fonte":         "inep",
                "indicador_id":  f"inep.{suffix}",
                "periodo":       periodo,
                "valor":         100.0 * soma / n,
                "valor_texto":   None,
                "unidade":       "percentual",
                "fonte_arquivo": f"censo_escolar_{ano}.zip",
            }
            emitted += 1
    print(f"  INEP {ano}: emitidos {emitted:,} indicadores em "
          f"{len(counts):,} munis", file=sys.stderr)


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
            ensure_fetched("inep_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/inep/raw/ já populado.",
                  file=sys.stderr)

    ibge_to_uf = _build_ibge_to_uf()
    print(f"INEP→ibge dim: {len(ibge_to_uf):,} munis válidos", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="inep")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def inep_indicadores() -> Iterator[dict]:
        anos_filter = set(args.ano) if args.ano else None
        for key in sorted(list_keys(S3_PREFIX)):
            name = key.rsplit("/", 1)[-1]
            m = ZIP_RE.match(name)
            if not m:
                continue
            ano = int(m.group(1))
            if anos_filter and ano not in anos_filter:
                continue
            print(f"  INEP {name}: download", file=sys.stderr)
            t0 = time.monotonic()
            zip_bytes = get_object_bytes(key)
            print(f"  INEP {name}: {len(zip_bytes)/1e6:.0f} MB em "
                  f"{time.monotonic()-t0:.1f}s", file=sys.stderr)
            t0 = time.monotonic()
            for rec in _iter_censo(zip_bytes, ano, ibge_to_uf):
                if ("inep", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec
            print(f"  INEP {ano}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="inep_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(inep_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC LIMIT 30"))
        return 0

    pipe = dlt.pipeline(pipeline_name="inep", destination=s3tables_iceberg)
    info = pipe.run(inep_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
