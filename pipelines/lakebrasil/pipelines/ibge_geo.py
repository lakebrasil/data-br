"""IBGE Localidades — regiões metropolitanas → data_br.indicadores_serie.

Lê `s3://data-br-raw/ibge/raw/regioes_metropolitanas.json` (84 RMs
oficiais com membros). Para cada (RM, município membro), emite 1 row:

  ibge_code     ← localidade.id (do município membro)
  fonte         ← 'ibge'
  indicador_id  ← `ibge.rm.{rm_slug}` (e.g. `ibge.rm.sao_paulo`)
  periodo       ← '2024' (ano do catálogo IBGE)
  valor         ← 1.0 (flag de pertencimento; AVG/SUM downstream dá count
                  de munis na RM)
  valor_texto   ← rm_nome (nome completo da RM)
  unidade       ← 'flag'

JSON structure (1 row por RM):
  {
    "id": "00101",
    "nome": "Região Metropolitana de Porto Velho",
    "UF": {...},
    "municipios": [{"id": 1100205, "nome": "Porto Velho"}, ...]
  }

Source: servicodados.ibge.gov.br/api/v1/localidades/regioes-metropolitanas

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.ibge_geo --no-fetch
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_KEY = "ibge/raw/regioes_metropolitanas.json"
PERIODO = "2024"  # catálogo IBGE atualizado em 2024 (RIDE Salvador add)


def _slug(text: str) -> str:
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    # Trim "Região Metropolitana de" / "RIDE" prefixos pra slug mais limpo.
    s = re.sub(r"^(regiao_metropolitana_de_|regiao_administrativa_integrada_de_desenvolvimento_de_|ride_)", "", s)
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")[:40]


def _build_uf_to_uf():
    """ibge_code 7-dígitos → uf sigla. Cached pra resolve uf das munis sem
    quebrar IBGE codes em 2 digitos."""
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    out: dict[int, str] = {}
    for ibge, uf in zip(arrow.column("ibge_code").to_pylist(),
                        arrow.column("uf").to_pylist()):
        if ibge is not None:
            out[int(ibge)] = uf
    return out


def _iter_rms(ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    raw = get_object_bytes(S3_KEY)
    data = json.loads(raw)
    print(f"  IBGE RM: {len(data)} regiões metropolitanas no catálogo",
          file=sys.stderr)
    emitted = 0
    skipped = 0
    for rm in data:
        rm_id = rm.get("id", "")
        rm_nome = rm.get("nome", "").strip()
        if not rm_nome:
            continue
        slug = _slug(rm_nome)
        for mun in rm.get("municipios", []):
            ibge = mun.get("id")
            if not ibge:
                continue
            ibge_int = int(ibge)
            uf = ibge_to_uf.get(ibge_int)
            if uf is None:
                skipped += 1
                continue
            yield {
                "ibge_code":     ibge_int,
                "uf":            uf,
                "fonte":         "ibge",
                "indicador_id":  f"ibge.rm.{slug}",
                "periodo":       PERIODO,
                "valor":         1.0,
                "valor_texto":   rm_nome,
                "unidade":       "flag",
                "fonte_arquivo": "regioes_metropolitanas.json",
            }
            emitted += 1
    print(f"  IBGE RM: emitted {emitted:,} (ibge,rm) pairs; "
          f"skipped {skipped} sem uf_match", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("ibge_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/ibge/raw/ já populado.",
                  file=sys.stderr)

    ibge_to_uf = _build_uf_to_uf()
    print(f"IBGE→UF índice: {len(ibge_to_uf):,} munis", file=sys.stderr)

    # Granularidade per-(indicador_id, periodo): se ALGUMAS RMs novas
    # aparecem no catálogo IBGE futuro, só elas são adicionadas no re-run.
    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def ibge_rm() -> Iterator[dict]:
        for rec in _iter_rms(ibge_to_uf):
            if ("ibge", rec["indicador_id"], rec["periodo"]) in already:
                continue
            yield rec

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="ibge_geo_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(ibge_rm())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT valor_texto AS rm_nome, count(*) AS munis "
                "FROM indicadores_serie GROUP BY valor_texto "
                "ORDER BY munis DESC LIMIT 10"))
        return 0

    t0 = time.monotonic()
    pipe = dlt.pipeline(pipeline_name="ibge_geo", destination=s3tables_iceberg)
    info = pipe.run(ibge_rm())
    print(info)
    print(f"  IBGE geo: load step em {time.monotonic()-t0:.1f}s", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
