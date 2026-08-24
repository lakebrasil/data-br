# web-data

Flat Parquet exports of the warehouse tables, served via jsDelivr's GitHub
CDN (`cdn.jsdelivr.net/gh/lakebrasil/data-br@<tag>/web-data/<table>.parquet`)
for [lakebrasil.dev/explore](https://lakebrasil.dev/explore) — a browser-side
DuckDB-WASM data explorer. jsDelivr sets `Access-Control-Allow-Origin: *`
and supports HTTP range requests, which GitHub Release assets don't;
raw git contents at a pinned ref is the only free/no-new-infra way to get
CORS-enabled, range-request-capable hosting straight from GitHub.

Deliberate tradeoff: this bloats the repo's git history (~280MB as of the
first snapshot, growing with each refresh) in exchange for not standing up
separate infrastructure. `comex_municipio` is split into two <100MB parts
(GitHub's hard per-file limit) — query it as
`read_parquet(['.../comex_municipio_part1.parquet', '.../comex_municipio_part2.parquet'])`.

Regenerate via `pipelines/scripts/export_parquet_for_web.py`, then tag the
commit (e.g. `web-data-YYYY-MM-DD`) and update the pinned ref in
`lakebrasil-site`'s `src/lib/duckdbTables.ts` — jsDelivr caches tags/commits
permanently, so a new snapshot needs a new tag, not just a new commit on
main.
