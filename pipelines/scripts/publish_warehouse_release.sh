#!/bin/bash
# Publishes the local Iceberg warehouse as a GitHub Release, one tarball
# per table + the shared SQLite catalog — see the "Zero-infra dev" self-host
# option in the README for how to consume this locally with DuckDB/pyiceberg.
#
# Usage: ./publish_warehouse_release.sh <warehouse_dir> <repo> <tag>
# Example:
#   ./publish_warehouse_release.sh \
#     /Users/nataliamesquita/datatech/lakebrasil-warehouse \
#     lakebrasil/data-br \
#     warehouse-2026-08-21
set -euo pipefail

WAREHOUSE="${1:?usage: publish_warehouse_release.sh <warehouse_dir> <repo> <tag>}"
REPO="${2:?missing repo}"
TAG="${3:?missing tag}"

STAGING=$(mktemp -d)
trap 'rm -rf "$STAGING"' EXIT

echo "staging tarballs in $STAGING"

# One tarball per Iceberg table (data/ + metadata/), named after the table.
for table_dir in "$WAREHOUSE"/data_br/*/; do
  table=$(basename "$table_dir")
  tarball="$STAGING/${table}.tar.gz"
  echo "  tar: $table"
  tar czf "$tarball" -C "$WAREHOUSE" "data_br/$table"
done

# The shared SQLite catalog — every table's metadata pointer lives here.
cp "$WAREHOUSE/catalog.db" "$STAGING/catalog.db"

echo ""
echo "assets:"
ls -lh "$STAGING" | tail -n +2

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  echo "release $TAG exists — uploading (clobber existing assets)"
  gh release upload "$TAG" "$STAGING"/*.tar.gz "$STAGING/catalog.db" \
    --repo "$REPO" --clobber
else
  echo "creating release $TAG"
  gh release create "$TAG" "$STAGING"/*.tar.gz "$STAGING/catalog.db" \
    --repo "$REPO" \
    --title "Warehouse snapshot — $TAG" \
    --notes "Local Iceberg warehouse snapshot (SQLite catalog + Parquet data), one tarball per table. Download catalog.db + the tables you want into the same directory, then \`export ICEBERG_WAREHOUSE=local://\$PWD\` — see the README's zero-infra self-host section."
fi

echo "done: https://github.com/$REPO/releases/tag/$TAG"
