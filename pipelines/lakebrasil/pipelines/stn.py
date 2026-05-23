"""Tesouro Nacional (STN) — transferências federais por município por mês.

Carrega 11 CSVs em `s3://data-br-raw/stn/raw/*-por-municipio.csv` →
`data_br.indicadores_serie` (~10M data points, fonte='stn').

Cada CSV tem formato wide: uma linha por (município, mês), colunas
1996-2026 com valor em R$. Pivot pra long: 1 row Iceberg por
(ibge_code, indicador_id, periodo='YYYY-MM').

Indicadores (filename stem → indicador_id):
  fpm                  → stn.fpm                  (Fundo de Participação dos Municípios)
  fundeb               → stn.fundeb               (Fundo de Desenvolvimento da Educação Básica)
  itr                  → stn.itr                  (Imposto Territorial Rural)
  cide                 → stn.cide                 (CIDE-Combustíveis)
  iof_gold             → stn.iof_gold             (IOF sobre ouro)
  fex                  → stn.fex                  (Fomento à Exportação)
  lc_176               → stn.lc_176               (LC 176 — comp. desoneração ICMS)
  lc_8796              → stn.lc_8796              (LC 87/96 — comp. desoneração ICMS)
  transferencias_total → stn.transferencias_total (mensal — soma das 4 últimas)

Source: https://www.tesourotransparente.gov.br/temas/transferencias-da-uniao
COD_MUN do CSV é código TSE (4-7 dígitos) — bate com `data_br.municipios.codigo_tse`
pra resolver ibge_code.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.stn --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.stn --indicador fpm --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
from typing import Iterator

import dlt

from lakebrasil.loaders.iceberg import catalog
from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "stn/raw/"

# Filename → (indicador_id, friendly nome). Filename pattern: `{stem}-por-municipio.csv`.
INDICADORES = {
    "fpm-por-municipio.csv":            ("stn.fpm",         "Fundo de Participação dos Municípios"),
    "fundeb-por-municipio.csv":         ("stn.fundeb",      "Fundo de Desenvolvimento da Educação Básica"),
    "itr-por-municipio.csv":            ("stn.itr",         "Imposto Territorial Rural"),
    "cide-por-municipio.csv":           ("stn.cide",        "CIDE-Combustíveis"),
    "iof-gold-por-municipio.csv":       ("stn.iof_gold",    "IOF sobre operações com ouro"),
    "fex-por-municipio.csv":            ("stn.fex",         "Auxílio Financeiro à Exportação"),
    "lc-176-por-municipio.csv":         ("stn.lc_176",      "LC 176/2020 (compensação ICMS)"),
    "lc-8796-por-municipio.csv":        ("stn.lc_8796",     "LC 87/96 (Lei Kandir)"),
}
# transferencias-mensal-municipios-YYYYMM.csv tem layout diferente
# (snapshot mensal, não wide-by-year) — não cobrir nessa primeira fatia.

# Decimal br: "1.234,56" → 1234.56
_NUM_RE = re.compile(r"^[\s\-]*[0-9.,]*\s*$")


def _parse_money_br(s: str | None) -> float | None:
    if not s:
        return None
    s = s.strip()
    if not s or s == "-" or not _NUM_RE.match(s):
        return None
    # Remove thousands sep '.', troca decimal ',' → '.'
    cleaned = s.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _build_tse_to_ibge() -> dict[str, int]:
    """codigo_tse na dim municipios → ibge_code. Mesmo padrão que tse.py
    mas separado pra evitar import cíclico. COD_MUN no STN vem zerado à
    esquerda (4 chars: '0643'); normalizamos via lstrip."""
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "codigo_tse")
    ).to_arrow()
    out: dict[str, int] = {}
    for ibge, tse in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("codigo_tse").to_pylist(),
    ):
        if tse is None:
            continue
        key = str(tse).strip().lstrip("0") or "0"
        out[key] = int(ibge)
    return out


def _iter_csv(s3_key: str, indicador_id: str,
              tse_to_ibge: dict[str, int]) -> Iterator[dict]:
    """Stream pivot do CSV STN (wide → long). Yields 1 row por
    (município, mes, ano) com valor não-null."""
    raw = get_object_bytes(s3_key)
    text_io = io.TextIOWrapper(io.BytesIO(raw), encoding="latin-1", newline="")
    reader = csv.DictReader(text_io, delimiter=";")
    fonte_arquivo = s3_key.rsplit("/", 1)[-1]
    misses = 0
    emitted = 0
    # Os field names a partir da 6ª coluna são anos ('1996', '1997', ...).
    year_cols = [c for c in reader.fieldnames or [] if c and c.isdigit() and len(c) == 4]
    for row in reader:
        cod = (row.get("COD_MUN") or "").strip().lstrip("0") or "0"
        ibge = tse_to_ibge.get(cod)
        if ibge is None:
            misses += 1
            continue
        try:
            mes = int((row.get("Mês") or "0").strip())
        except ValueError:
            continue
        if not 1 <= mes <= 12:
            continue
        uf = (row.get("UF") or "").strip().upper() or None
        for ano in year_cols:
            valor = _parse_money_br(row.get(ano))
            if valor is None or valor == 0:
                # Filtra zeros — STN preenche com 0 quando não houve transferência.
                # Manter zero perde sinal real (município elegível, valor 0?) — tradeoff,
                # mas reduz volume ~3×.
                continue
            yield {
                "ibge_code":     ibge,
                "uf":            uf,
                "fonte":         "stn",
                "indicador_id":  indicador_id,
                "periodo":       f"{ano}-{mes:02d}",
                "valor":         valor,
                "valor_texto":   None,
                "unidade":       "BRL",
                "fonte_arquivo": fonte_arquivo,
            }
            emitted += 1
    print(f"  STN {fonte_arquivo}: emitted={emitted:,} misses={misses}",
          file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--indicador", choices=sorted(set(v[0] for v in INDICADORES.values())),
                   help="Carrega só um indicador (debug). Default: todos.")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("stn_*", refresh=args.refresh)
        except ValueError as e:
            print(f"  [warn] {e}", file=sys.stderr)
            print("  [warn] assumindo s3://data-br-raw/stn/raw/ já populado.",
                  file=sys.stderr)

    tse_to_ibge = _build_tse_to_ibge()
    print(f"STN→IBGE índice: {len(tse_to_ibge):,} municípios", file=sys.stderr)

    # Idempotência fina: pula (fonte, indicador_id, periodo) já carregados.
    # Granularidade per-indicador permite resume mid-arquivo se interrompido —
    # re-run só processa indicadores não-comitados.
    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def stn_indicadores() -> Iterator[dict]:
        keys = sorted(list_keys(S3_PREFIX))
        for key in keys:
            name = key.rsplit("/", 1)[-1]
            spec = INDICADORES.get(name)
            if spec is None:
                continue
            indicador_id, _ = spec
            if args.indicador and indicador_id != args.indicador:
                continue
            t0 = time.monotonic()
            for rec in _iter_csv(key, indicador_id, tse_to_ibge):
                if ("stn", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec
            print(f"  STN {indicador_id}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="stn_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(stn_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*) FROM indicadores_serie "
                "GROUP BY indicador_id ORDER BY count(*) DESC"))
        return 0

    pipe = dlt.pipeline(pipeline_name="stn", destination=s3tables_iceberg)
    info = pipe.run(stn_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
