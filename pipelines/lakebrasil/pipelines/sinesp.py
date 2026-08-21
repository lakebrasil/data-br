"""SINESP — eventos de segurança pública por município → indicadores_serie.

Carrega `s3://data-br-raw/sinesp/raw/basededadosvde.zip` (10 anos
2015-2024 em CSVs anuais, ~600 MB descompactado). Cada CSV tem 1 row
por (uf, município, evento, data_referencia, agente, arma, ...) =
ocorrência individual. Agregamos para nível município-mês:
  GROUP BY (uf, municipio, evento, ano-mes)
  SUM(total_vitima) → valor

Eventos padrão (~25 categorias): homicídio doloso, feminicídio,
roubo, latrocínio, suicídio, mortes no trânsito, etc.

Mapeamento → `data_br.indicadores_serie`:
  ibge_code     ← resolve_ibge(uf, municipio) (slug match)
  fonte         ← 'sinesp'
  indicador_id  ← `sinesp.{evento_slug}` (e.g. `sinesp.homicidio_doloso`)
  periodo       ← YYYY-MM
  valor         ← SUM(total_vitima) ou COUNT(*) se total_vitima ausente
  unidade       ← 'ocorrencias'

Source: gov.br/mj/dados-abertos/sinesp.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sinesp --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.sinesp --year 2024 --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import unicodedata
import zipfile
from collections import defaultdict
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.enrich import municipios_count, resolve_ibge
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "sinesp/raw/"
ZIP_FILE = "basededadosvde.zip"
INNER_RE = re.compile(r"^BancoVDE (\d{4})\.csv$")


def _slug(text: str) -> str:
    """Normaliza nome de evento → indicador slug ASCII safe.
    Ex: 'Homicídio doloso' → 'homicidio_doloso'."""
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")[:40]


def _parse_date_br(s: str | None) -> str | None:
    """1/1/2024 → 2024-01 (YYYY-MM)."""
    if not s:
        return None
    parts = s.strip().split("/")
    if len(parts) != 3:
        return None
    try:
        _, m, y = int(parts[0]), int(parts[1]), int(parts[2])
        return f"{y:04d}-{m:02d}"
    except ValueError:
        return None


def _iter_year_csv(zip_bytes: bytes, ano: int) -> Iterator[dict]:
    """Stream BancoVDE {ano}.csv → 1 row por (uf, mun, evento, periodo)
    pré-agregado em memória."""
    target = f"BancoVDE {ano}.csv"
    accum: dict[tuple[str, str, str, str], int] = defaultdict(int)
    rows_seen = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        if target not in zf.namelist():
            return
        with zf.open(target) as fh:
            # SINESP CSVs vêm em utf-8 (com BOM ou sem) ou latin-1
            # dependendo do ano. Tentar utf-8 com replace evita
            # UnicodeDecodeError em bytes 0xc2-0xff órfãos sem perder
            # quase nenhum caractere útil (gov.br não usa Unicode raro
            # em nomes de município).
            text_io = io.TextIOWrapper(fh, encoding="utf-8-sig",
                                       newline="", errors="replace")
            reader = csv.DictReader(text_io, delimiter=";")
            for r in reader:
                rows_seen += 1
                uf = (r.get("uf") or "").strip().upper()
                mun = (r.get("municipio") or "").strip()
                evento = (r.get("evento") or "").strip()
                periodo = _parse_date_br(r.get("data_referencia"))
                if not (uf and mun and evento and periodo):
                    continue
                # Use total_vitima quando disponível, senão soma 1 (cada
                # row é 1 ocorrência registrada).
                vitima = r.get("total_vitima") or "0"
                try:
                    qtd = int(vitima.strip()) if vitima.strip() else 1
                except ValueError:
                    qtd = 1
                if qtd == 0:
                    qtd = 1
                accum[(uf, mun, evento, periodo)] += qtd
    print(f"  SINESP {ano}: {rows_seen:,} rows lidas → {len(accum):,} (uf,mun,evento,mes) "
          f"agregados", file=sys.stderr)
    skipped_ibge = 0
    for (uf, mun, evento, periodo), qtd in accum.items():
        ibge = resolve_ibge(uf, mun)
        if ibge is None:
            # Schema indicadores_serie requer ibge_code NOT NULL; skipa
            # ocorrências onde slug match falhou (geralmente municípios
            # com grafia não-canônica ou agregados estaduais).
            skipped_ibge += 1
            continue
        yield {
            "ibge_code":     ibge,
            "uf":            uf,
            "fonte":         "sinesp",
            "indicador_id":  f"sinesp.{_slug(evento)}",
            "periodo":       periodo,
            "valor":         float(qtd),
            "valor_texto":   None,
            "unidade":       "ocorrencias",
            "fonte_arquivo": ZIP_FILE,
        }
    if skipped_ibge:
        print(f"  SINESP {ano}: skipped {skipped_ibge:,} (uf,mun) sem ibge_match",
              file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--year", type=int, help="YYYY single year (debug).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("sinesp_*", refresh=args.refresh)
        except ValueError as e:
            print(f"  [warn] {e}", file=sys.stderr)
            print("  [warn] assumindo s3://data-br-raw/sinesp/raw/ já populado.",
                  file=sys.stderr)

    municipios_count()  # warm dim cache pra resolve_ibge
    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def sinesp_indicadores() -> Iterator[dict]:
        keys = sorted(list_keys(S3_PREFIX))
        zip_key = next((k for k in keys if k.endswith(ZIP_FILE)), None)
        if zip_key is None:
            print(f"  [warn] {ZIP_FILE} não encontrado em raw", file=sys.stderr)
            return
        t0 = time.monotonic()
        zip_bytes = get_object_bytes(zip_key)
        print(f"  SINESP zip carregado em {time.monotonic()-t0:.1f}s "
              f"({len(zip_bytes)/1e6:.0f} MB)", file=sys.stderr)
        # Descobre anos disponíveis dentro do zip.
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            anos = sorted({
                int(m.group(1)) for n in zf.namelist()
                if (m := INNER_RE.match(n))
            })
        print(f"  SINESP anos no zip: {anos}", file=sys.stderr)
        for ano in anos:
            if args.year and ano != args.year:
                continue
            t0 = time.monotonic()
            for rec in _iter_year_csv(zip_bytes, ano):
                if ("sinesp", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec
            print(f"  SINESP {ano}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="sinesp_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(sinesp_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), sum(valor) "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC LIMIT 10"))
        return 0

    pipe = dlt.pipeline(pipeline_name="sinesp", destination=s3tables_iceberg)
    info = pipe.run(sinesp_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
