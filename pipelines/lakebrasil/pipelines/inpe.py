"""INPE PRODES — Desmatamento Amazônia → indicadores_serie.

Lê `s3://data-br-raw/inpe/raw/prodes_amazon_yearly_full.geojson` (polígonos
de desmatamento anuais com geometria multipolygon, ~800K features —
full PRODES series, paginado via WFS `startIndex` pelo fetcher
`geoserver_wfs` porque o GeoServer da TerraBrasilis hard-cap a 50K
features/response independente do `count` pedido) + `amazon-municipalities.zip`
(shapefile dos 559 municípios do bioma Amazônia com `geocodigo`=IBGE 7-dig,
via WFS layer `prodes-amazon-nb:municipalities_amazon_biome`).

Spatial join: PRODES centroids → município contendo. Reproject pra
SIRGAS 2000 Brazil Polyconic (EPSG:5880) pra usar áreas/centroids
corretamente.

Agrega área desmatada por (município, ano) em km²:
  inpe.prodes_desmatamento_km2_ano       área desmatada no ano N

Cobre 2008-2024 × 559 munis Amazônia.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.inpe --no-fetch
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import time
import warnings
import zipfile
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import get_object_bytes
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PRODES = "inpe/raw/prodes_amazon_yearly_full.geojson"
S3_MUNIS  = "inpe/raw/amazon-municipalities.zip"

warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS")


def _build_ibge_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    return {int(i): u or "??" for i, u in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
    ) if i is not None}


def _iter_prodes(ibge_to_uf: dict[int, str]) -> Iterator[dict]:
    import geopandas as gpd
    from shapely.ops import transform as _shapely_transform

    t0 = time.monotonic()
    # Munis da Amazônia (shapefile, gerado pelo WFS SHAPE-ZIP do
    # GeoServer da TerraBrasilis).
    raw_munis = get_object_bytes(S3_MUNIS)
    with tempfile.TemporaryDirectory() as td:
        with zipfile.ZipFile(io.BytesIO(raw_munis)) as zf:
            zf.extractall(td)
        munis = gpd.read_file(os.path.join(td, "municipalities_amazon_biome.shp"))
    # O .prj que o GeoServer grava nesse SHAPE-ZIP declara eixo
    # EPSG-compliant (latitude, longitude) em vez da ordem tradicional
    # de shapefile (X=lon, Y=lat) — GDAL/pyogrio honra essa declaração
    # ao ler e entrega geometrias com x/y trocados (bounds saem tipo
    # [-16.66, -73.98, 5.27, -43.40], i.e. lat nas colunas de x). Sem
    # corrigir, o sjoin com PRODES (lon/lat normal) não bate quase nada
    # (~2 hits em 800K). Troca x<->y manualmente e reforça o CRS.
    munis["geometry"] = munis["geometry"].apply(
        lambda geom: _shapely_transform(lambda x, y: (y, x), geom)
    )
    munis = munis.set_crs("EPSG:4674", allow_override=True)
    print(f"  INPE munis Amazônia: {len(munis):,} ({time.monotonic()-t0:.1f}s)",
          file=sys.stderr)

    t0 = time.monotonic()
    raw_prodes = get_object_bytes(S3_PRODES)
    print(f"  INPE prodes: {len(raw_prodes)/1e6:.0f} MB raw "
          f"({time.monotonic()-t0:.1f}s)", file=sys.stderr)
    t0 = time.monotonic()
    # Suporta .zip (extract .gpkg) ou .geojson direto
    if S3_PRODES.endswith(".zip"):
        with tempfile.TemporaryDirectory() as td:
            with zipfile.ZipFile(io.BytesIO(raw_prodes)) as zf:
                zf.extractall(td)
            inner = next((os.path.join(td, n) for n in os.listdir(td)
                          if n.lower().endswith((".gpkg", ".shp", ".geojson"))), None)
            if not inner:
                raise FileNotFoundError(f"sem .gpkg/.shp/.geojson dentro de {S3_PRODES}")
            prodes = gpd.read_file(inner)
    else:
        prodes = gpd.read_file(io.BytesIO(raw_prodes))
    print(f"  INPE prodes: {len(prodes):,} features parsed "
          f"({time.monotonic()-t0:.1f}s)", file=sys.stderr)

    # Reproject pra SIRGAS 2000 Brazil Polyconic (EPSG:5880)
    t0 = time.monotonic()
    munis_p = munis.to_crs(epsg=5880)
    prodes_p = prodes.to_crs(epsg=5880)
    prodes_p["centroid"] = prodes_p.geometry.centroid
    prodes_centroids = prodes_p.set_geometry("centroid")[
        ["centroid", "year", "area_km"]]
    joined = gpd.sjoin(prodes_centroids,
                       munis_p[["geocodigo", "geometry"]],
                       how="left", predicate="within")
    hits = int(joined["geocodigo"].notna().sum())
    print(f"  INPE sjoin: {hits:,}/{len(joined):,} hit ({time.monotonic()-t0:.1f}s)",
          file=sys.stderr)

    # Agg por (geocodigo, year) - usa area_km do PRODES (já em km²)
    agg = (joined.dropna(subset=["geocodigo"])
                  .groupby(["geocodigo", "year"])["area_km"].sum()
                  .reset_index())
    print(f"  INPE agg: {len(agg):,} (muni,ano) pairs", file=sys.stderr)

    emitted = 0
    for _, row in agg.iterrows():
        try:
            ibge = int(row["geocodigo"])
        except (TypeError, ValueError):
            continue
        if ibge not in ibge_to_uf:
            continue
        uf = ibge_to_uf.get(ibge, "??")
        yield {
            "ibge_code": ibge, "uf": uf,
            "fonte": "inpe",
            "indicador_id": "inpe.prodes_desmatamento_km2_ano",
            "periodo": str(int(row["year"])),
            "valor": float(row["area_km"]),
            "valor_texto": None, "unidade": "km²",
            "fonte_arquivo": "prodes_amazon_yearly_full.geojson",
        }
        emitted += 1
    print(f"  INPE emitidos: {emitted:,} rows", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("inpe_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://.../inpe/raw/ já populado.",
                  file=sys.stderr)

    ibge_to_uf = _build_ibge_to_uf()
    print(f"INPE→ibge dim: {len(ibge_to_uf):,} munis válidos", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="inpe")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def inpe_indicadores() -> Iterator[dict]:
        for rec in _iter_prodes(ibge_to_uf):
            if ("inpe", rec["indicador_id"], rec["periodo"]) in already:
                continue
            yield rec

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="inpe_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(inpe_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT periodo, count(*), sum(valor) "
                "FROM indicadores_serie GROUP BY periodo ORDER BY periodo"))
        return 0

    pipe = dlt.pipeline(pipeline_name="inpe", destination=s3tables_iceberg)
    info = pipe.run(inpe_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
