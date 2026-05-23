"""ANP preços combustível por município → data_br.indicadores_serie.

Carrega CSVs em `s3://data-br-raw/anp/raw/precos-{produto}-{ano-mes}.csv`
(coletas semanais ANP nos postos revendedores) → agrega para nível
município-mês-produto:

    GROUP BY (uf, municipio, produto, ano-mes)
    AVG(valor_venda) → valor

Mapeamento → indicadores_serie:
  ibge_code     ← resolve_ibge(uf, municipio)
  fonte         ← 'anp'
  indicador_id  ← `anp.preco_{produto_slug}` (e.g. `anp.preco_diesel`, `anp.preco_gnv`)
  periodo       ← YYYY-MM da coleta
  valor         ← AVG(valor_venda) — R$/litro ou R$/m³
  unidade       ← 'R$/litro' | 'R$/m³'

Produtos típicos no CSV (coluna "Produto"):
  DIESEL, DIESEL S10, GASOLINA, GASOLINA ADITIVADA, ETANOL, GLP, GNV

Source: gov.br/anp Levantamento de Preços de Combustíveis.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.anp_precos --no-fetch
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
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import municipios_count, resolve_ibge
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "anp/raw/"
# Filename pattern: precos-{produto}-{YYYY-MM}.csv (sometimes
# `precos-diesel-gnv-{YYYY-MM}.csv` — múltiplos produtos no mesmo arquivo).
FILE_RE = re.compile(r"^precos-[a-z0-9-]+?-(\d{4})-(\d{2})\.csv$")


def _slug(text: str) -> str:
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")[:40]


def _parse_money_br(s: str | None) -> float | None:
    if not s or not s.strip():
        return None
    s = s.strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _iter_csv(s3_key: str, ano: int, mes: int) -> Iterator[dict]:
    """Stream CSV → (uf, mun, produto, periodo) → AVG(valor_venda) pré-agregado."""
    name = s3_key.rsplit("/", 1)[-1]
    print(f"  ANP {name}: load", file=sys.stderr)
    raw = get_object_bytes(s3_key)
    text_io = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8-sig",
                               newline="", errors="replace")
    reader = csv.DictReader(text_io, delimiter=";")
    # Acumulador (uf, mun, produto) → (sum, n)
    accum: dict[tuple[str, str, str, str], tuple[float, int]] = defaultdict(
        lambda: (0.0, 0)
    )
    rows_seen = 0
    rows_skip = 0
    periodo = f"{ano:04d}-{mes:02d}"
    for r in reader:
        rows_seen += 1
        uf = (r.get("Estado - Sigla") or "").strip().upper()
        mun = (r.get("Municipio") or "").strip()
        produto = (r.get("Produto") or "").strip()
        if not (uf and mun and produto):
            rows_skip += 1
            continue
        valor = _parse_money_br(r.get("Valor de Venda"))
        if valor is None or valor <= 0:
            rows_skip += 1
            continue
        unidade = (r.get("Unidade de Medida") or "").strip()
        key = (uf, mun, produto, unidade)
        soma, n = accum[key]
        accum[key] = (soma + valor, n + 1)
    print(f"  ANP {name}: rows={rows_seen:,} skip={rows_skip:,} → {len(accum):,} "
          f"chaves (uf,mun,produto)", file=sys.stderr)

    skipped_ibge = 0
    for (uf, mun, produto, unidade), (soma, n) in accum.items():
        ibge = resolve_ibge(uf, mun)
        if ibge is None:
            skipped_ibge += 1
            continue
        yield {
            "ibge_code":     ibge,
            "uf":            uf,
            "fonte":         "anp",
            "indicador_id":  f"anp.preco_{_slug(produto)}",
            "periodo":       periodo,
            "valor":         soma / n,
            "valor_texto":   None,
            "unidade":       unidade or "R$/litro",
            "fonte_arquivo": name,
        }
    if skipped_ibge:
        print(f"  ANP {name}: skipped {skipped_ibge:,} (uf,mun) sem ibge_match",
              file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("anp_precos_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/anp/raw/precos-*.csv já populado.",
                  file=sys.stderr)

    municipios_count()  # warm dim
    # Filtro per-indicador permite re-run pegar só produtos novos sem
    # reprocessar o CSV inteiro (anp.preco_diesel, anp.preco_gasolina, ...).
    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def anp_precos() -> Iterator[dict]:
        for key in sorted(list_keys(S3_PREFIX)):
            name = key.rsplit("/", 1)[-1]
            m = FILE_RE.match(name)
            if not m:
                continue
            ano, mes = int(m.group(1)), int(m.group(2))
            t0 = time.monotonic()
            periodo = f"{ano:04d}-{mes:02d}"
            for rec in _iter_csv(key, ano, mes):
                if ("anp", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec
            print(f"  ANP {periodo}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="anp_precos_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(anp_precos())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), avg(valor) AS preco_medio "
                "FROM indicadores_serie GROUP BY indicador_id ORDER BY count(*) DESC LIMIT 10"))
        return 0

    pipe = dlt.pipeline(pipeline_name="anp_precos", destination=s3tables_iceberg)
    info = pipe.run(anp_precos())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
