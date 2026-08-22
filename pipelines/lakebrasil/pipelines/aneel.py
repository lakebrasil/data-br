"""ANEEL SIGA — empreendimentos de geração elétrica → data_br.indicadores_serie.

Carrega `s3://data-br-raw/aneel/raw/siga-empreendimentos-geracao.csv`
(snapshot SIGA com todos os empreendimentos cadastrados na ANEEL,
inclui operação + construção + outorgada). Filtra para apenas
\"Operação\" e agrega potência outorgada (kW) por (município, tipo_geracao):

  GROUP BY (uf, municipio, tipo_geracao)
  SUM(potencia_outorgada_kw) → valor

Tipos geração ANEEL: UHE, PCH, CGH, EOL, UFV, UTE, UTN

Mapeamento → indicadores_serie:
  ibge_code     ← resolve_ibge(uf, municipio_first)
  fonte         ← 'aneel'
  indicador_id  ← `aneel.potencia_{tipo}_kw` + `aneel.potencia_total_kw`
                  + `aneel.qtd_{tipo}`
  periodo       ← '2026' (snapshot date)
  valor         ← SUM(potencia_outorgada_kw) ou COUNT(*)
  unidade       ← 'kW' ou 'empreendimentos'

DscMuninicpios pode ter múltiplas munis (\"Cidade1 - UF; Cidade2 - UF\")
em hidroelétricas que cruzam rio. Usamos só a primeira — empreendimentos
multi-município são <5% do total.

Source: dadosabertos.aneel.gov.br/dataset/siga-sistema-de-informacoes-de-geracao.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.aneel --no-fetch
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import unicodedata
from collections import defaultdict
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import municipios_count, resolve_ibge
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_KEY = "aneel/raw/siga-empreendimentos-geracao.csv"
PERIODO = "2026"  # SIGA é snapshot atual; sem histórico mensal


def _slug(text: str) -> str:
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower().strip()).strip("_")[:40]


def _parse_kw_br(s: str | None) -> float | None:
    if not s or not s.strip():
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_primeira_muni(s: str | None) -> str | None:
    """DscMuninicpios pode ser \"Nova Lima - MG\" ou
    \"Cidade1 - UF; Cidade2 - UF\" — pega só a primeira."""
    if not s or not s.strip():
        return None
    first = s.split(";")[0].split(",")[0].strip()
    # Format: \"MUNICIPIO - UF\". Tira o \" - UF\" no fim.
    if " - " in first:
        first = first.rsplit(" - ", 1)[0].strip()
    return first or None


def _iter_csv() -> Iterator[dict]:
    """Stream SIGA → (uf, municipio, tipo) → (sum_kw, count) accum."""
    raw = get_object_bytes(S3_KEY)
    print(f"  ANEEL SIGA: {len(raw)/1e6:.1f} MB raw", file=sys.stderr)
    text_io = io.TextIOWrapper(io.BytesIO(raw), encoding="latin-1",
                               newline="", errors="replace")
    reader = csv.DictReader(text_io, delimiter=";")
    # accum[(uf, mun, tipo)] = (soma_kw, count)
    accum: dict[tuple[str, str, str], tuple[float, int]] = defaultdict(
        lambda: (0.0, 0)
    )
    rows_seen = 0
    rows_skip_fase = 0
    rows_skip_mun = 0
    rows_skip_kw = 0
    for r in reader:
        rows_seen += 1
        fase = (r.get("DscFaseUsina") or "").strip()
        if not fase.lower().startswith("opera"):
            rows_skip_fase += 1
            continue
        uf = (r.get("SigUFPrincipal") or "").strip().upper()
        tipo = (r.get("SigTipoGeracao") or "").strip().upper()
        mun = _parse_primeira_muni(r.get("DscMuninicpios"))
        if not (uf and tipo and mun):
            rows_skip_mun += 1
            continue
        kw = _parse_kw_br(r.get("MdaPotenciaOutorgadaKw"))
        if kw is None or kw <= 0:
            rows_skip_kw += 1
            continue
        key = (uf, mun, tipo)
        soma, n = accum[key]
        accum[key] = (soma + kw, n + 1)
    print(f"  ANEEL SIGA: rows={rows_seen:,} skip_fase={rows_skip_fase:,} "
          f"skip_mun={rows_skip_mun:,} skip_kw={rows_skip_kw:,} → "
          f"{len(accum):,} (uf,mun,tipo)", file=sys.stderr)

    skipped_ibge = 0
    for (uf, mun, tipo), (soma_kw, n) in accum.items():
        ibge = resolve_ibge(uf, mun)
        if ibge is None:
            skipped_ibge += 1
            continue
        # Emit 2 indicators per (município, tipo): potência + count
        yield {
            "ibge_code":     ibge,
            "uf":            uf,
            "fonte":         "aneel",
            "indicador_id":  f"aneel.potencia_{_slug(tipo)}_kw",
            "periodo":       PERIODO,
            "valor":         soma_kw,
            "valor_texto":   None,
            "unidade":       "kW",
            "fonte_arquivo": "siga-empreendimentos-geracao.csv",
        }
        yield {
            "ibge_code":     ibge,
            "uf":            uf,
            "fonte":         "aneel",
            "indicador_id":  f"aneel.qtd_{_slug(tipo)}",
            "periodo":       PERIODO,
            "valor":         float(n),
            "valor_texto":   None,
            "unidade":       "empreendimentos",
            "fonte_arquivo": "siga-empreendimentos-geracao.csv",
        }
    if skipped_ibge:
        print(f"  ANEEL: skipped {skipped_ibge:,} (uf,mun) sem ibge_match",
              file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        ensure_fetched("aneel", refresh=args.refresh)

    municipios_count()  # warm dim
    # Granularidade fina: filtra per-indicador. Permite re-run pegando
    # apenas indicadores novos (ex: ANEEL adicionar tipo de geração novo).
    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def aneel_siga() -> Iterator[dict]:
        for rec in _iter_csv():
            if ("aneel", rec["indicador_id"], rec["periodo"]) in already:
                continue
            yield rec

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="aneel_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(aneel_siga())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*) AS munis, sum(valor) AS total "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY total DESC LIMIT 15"))
        return 0

    t0 = time.monotonic()
    pipe = dlt.pipeline(pipeline_name="aneel", destination=s3tables_iceberg)
    info = pipe.run(aneel_siga())
    print(info)
    print(f"  ANEEL: load step em {time.monotonic()-t0:.1f}s", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
