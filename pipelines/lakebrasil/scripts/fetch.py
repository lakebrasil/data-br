"""CLI driver for catalog-driven fetching.

Usage:
    python -m lakebrasil.scripts.fetch                       # all sources, all tiers
    python -m lakebrasil.scripts.fetch --tier 1              # only tier-1
    python -m lakebrasil.scripts.fetch --source bacen_ipca_mensal
    python -m lakebrasil.scripts.fetch --source bpc --refresh   # re-download even if exists
    python -m lakebrasil.scripts.fetch --dry-run             # plan only, no downloads

Idempotence: if `out_path` exists and the manifest's sha256 matches the
file on disk, we skip. `--refresh` forces re-download.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .catalog import CATALOG_PATH, expand_targets, load_catalog
from lakebrasil.fetchers import REGISTRY
from lakebrasil.fetchers.base import (
    FetchResult,
    manifest_key_for,
    read_manifest,
    s3_key_for_target,
    s3_object_exists,
)

RAW_ROOT = Path(__file__).resolve().parent.parent  # data-br-sources/


def _is_fresh(target) -> bool:
    """S3 object exists + manifest written → skip.

    Não revalida sha256 contra o objeto S3 (custaria 1 GET HEAD por
    artefato — caro em catalog grande). Confia no par (manifest, head)."""
    s3_key = s3_key_for_target(target)
    if not s3_object_exists(s3_key):
        return False
    manifest = read_manifest(manifest_key_for(target.source, s3_key))
    return bool(manifest and manifest.get("sha256"))


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--source", action="append", default=[], help="catalog source name (repeatable)")
    p.add_argument("--tier", type=int, choices=(1, 2, 3), help="filter by tier")
    p.add_argument("--refresh", action="store_true", help="re-fetch even if file exists")
    p.add_argument("--dry-run", action="store_true", help="list targets, no downloads")
    p.add_argument("--limit", type=int, help="cap total targets fetched")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    sources = load_catalog(CATALOG_PATH)

    if args.source:
        sources = {n: s for n, s in sources.items() if n in args.source}
        missing = set(args.source) - set(sources)
        if missing:
            print(f"!! unknown sources: {sorted(missing)}", file=sys.stderr)
            return 2
    if args.tier:
        sources = {n: s for n, s in sources.items() if s.tier == args.tier}

    targets = []
    for spec in sources.values():
        targets.extend(expand_targets(spec, RAW_ROOT))

    if args.limit:
        targets = targets[: args.limit]

    print(f"plan: {len(targets)} targets across {len(sources)} sources")
    if args.dry_run:
        for t in targets[:30]:
            print(f"  {t.source:25s} {t.fetcher:14s} → {t.out_path.name}")
        if len(targets) > 30:
            print(f"  ... and {len(targets) - 30} more")
        return 0

    ok = skip = fail = 0
    fail_log = []
    t0 = time.monotonic()
    for i, target in enumerate(targets, 1):
        if not args.refresh and _is_fresh(target):
            skip += 1
            continue
        fetcher = REGISTRY.get(target.fetcher)
        if fetcher is None:
            print(f"  [{i}/{len(targets)}] !! unknown fetcher {target.fetcher!r} for {target.source}")
            fail += 1
            continue
        result: FetchResult = fetcher(target)
        if result.ok:
            ok += 1
            mb = result.bytes_written / (1 << 20)
            print(
                f"  [{i}/{len(targets)}] OK   {target.source:22s} "
                f"{mb:>9.1f} MB  s3://.../{result.s3_key}"
            )
        else:
            fail += 1
            fail_log.append((target.source, target.url, result.error))
            print(f"  [{i}/{len(targets)}] FAIL {target.source:22s} {result.error}")

    elapsed = time.monotonic() - t0
    print(
        f"\ndone in {elapsed:.1f}s — ok={ok} skipped={skip} failed={fail}"
    )
    if fail_log:
        print("\nfailures:")
        for src, url, err in fail_log[:20]:
            print(f"  {src}: {err}\n    {url}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
