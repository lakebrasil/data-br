# lakebrasil/pipelines — architecture

Cloud-native ELT: declarative catalog → streaming fetcher → S3 raw →
dlt pipeline (extract/enrich) → S3 Tables (managed Iceberg) via pyiceberg.

## Layout

```
pipelines/
├── lakebrasil/
│   ├── scripts/
│   │   ├── catalog.yaml      # source catalog (schedule, license, tier, url pattern/generators)
│   │   ├── catalog.py        # parser + generator expansion (monthly_range, yearly_range, ufs, ...)
│   │   ├── fetch.py          # CLI: python -m lakebrasil.scripts.fetch
│   │   └── govbr_fetch.py    # standalone gov.br F5-bypass fetch helper
│   ├── fetchers/             # http, bcb_sgs, transparencia, govbr, webdav
│   ├── common/
│   │   ├── fetch.py          # ensure_fetched() bridge pipelines call into
│   │   ├── s3.py              # boto3 S3 helpers (raw bucket, multipart upload, manifests)
│   │   ├── enrich.py          # municipios dim + resolve_ibge(uf, nome)
│   │   ├── sidra.py           # IBGE SIDRA JSON -> indicadores_serie records
│   │   ├── incremental.py     # per-key max-value tracking for incremental loads
│   │   ├── args.py            # add_common_args() — shared CLI flags every pipeline exposes
│   │   └── csv.py             # streaming CSV helpers
│   ├── pipelines/
│   │   ├── destinations/
│   │   │   └── iceberg.py    # custom dlt destination -> pyiceberg append (S3 Tables or any REST catalog)
│   │   └── <source>.py       # one module per source, e.g. bacen.py -> data_br.macro_serie
│   ├── dq/                   # per-table data-quality checks + runner
│   └── loaders/
│       └── iceberg.py        # catalog() — pluggable Iceberg REST catalog connection (see below)
```

## Iceberg catalog backends (pluggable via `ICEBERG_WAREHOUSE`)

`lakebrasil.loaders.iceberg.catalog()` auto-detects the backend from the
`ICEBERG_WAREHOUSE` env var's format:

| Format | Backend |
|---|---|
| `arn:aws:s3tables:<region>:<account>:bucket/<name>` | AWS S3 Tables (native Iceberg REST endpoint, SigV4) |
| `http(s)://...` (+ `ICEBERG_REST_ENDPOINT`) | Vanilla REST catalog (Nessie, Lakekeeper, Tabular, ...) |
| `local:///abs/path/warehouse` | SQLite catalog + local filesystem (zero-infra dev — see `docker-compose.yml`) |

**S3 Tables auth gotcha**: the native S3 Tables Iceberg REST endpoint does
**not** vend temporary per-table credentials for standalone IAM roles (it's
`s3tables IAM authorization only` — confirmed empirically, not documented
clearly by AWS). `_build_s3tables_config()` resolves the caller's own boto3
credentials and passes them to pyiceberg explicitly rather than relying on
vending. The caller's principal still needs real `s3tables:*` IAM
permissions — this doesn't bypass authorization.

Tables are **created automatically** on first write per org/pipeline
(`DATA_BR_AUTOCREATE=1`, the default) — schema is inferred from the first
Arrow batch's columns via a generated Iceberg `NameMapping`. Set
`DATA_BR_AUTOCREATE=0` to require the table to already exist instead.

## Local dev — zero AWS

```bash
docker compose up -d   # MinIO (S3) + Nessie (Iceberg REST catalog) on non-default ports
export ICEBERG_WAREHOUSE=warehouse
export ICEBERG_REST_ENDPOINT=http://localhost:19120/iceberg/
export S3_ENDPOINT_URL=http://localhost:9100
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

pip install -e .
lakebrasil run bacen --dry-run   # stages to local DuckDB, no writes
lakebrasil run bacen              # real write, against the local MinIO+Nessie stack
```

## Storage layout (AWS backend)

```
s3://<DATA_BR_RAW_BUCKET>/
├── {source}/raw/{file}                       # raw artifacts (CSV, JSON, zip)
└── _manifest/{source}/{stem}.manifest.json   # url, sha256, bytes, fetched_at

S3 Tables warehouse:
└── data_br.{table}   # one Iceberg table per pipeline's dlt resource
```

## Cycle, per pipeline

Every pipeline is one invocation: `lakebrasil run <source> [flags]` (or
`python -m lakebrasil.pipelines.<source>` directly).

1. **fetch** (catalog-driven) — `ensure_fetched("<source-pattern>")` reads
   `catalog.yaml`, dispatches to the right fetcher (http / bcb_sgs /
   transparencia / govbr / webdav), streams URL → S3 multipart upload,
   writes a manifest with sha256. Idempotent — skips if the manifest
   already matches.
2. **dlt extract** — `@dlt.resource` lists/streams each raw file from S3,
   normalizes types.
3. **enrichment** — pipelines keyed by `(uf, município)` text resolve
   `ibge_code` via `resolve_ibge()` (slug match against the municípios
   dimension).
4. **dlt load → custom destination** (`destinations/iceberg.py`) — receives
   a `pa.RecordBatch`, casts/reorders to the Iceberg table's schema,
   `pyiceberg_table.append()`.

Common flags (every pipeline, via `add_common_args`):
- `--no-fetch` — skip step 1 (raw already staged in S3)
- `--refresh` — force re-download even if the manifest is fresh
- `--dry-run` — stage to local DuckDB only, no Iceberg write
- `--table <name>` — override the target table name (handy for a throwaway test table)
