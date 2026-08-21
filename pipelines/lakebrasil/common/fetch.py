"""Fetch helper for end-to-end pipelines.

Each `_pipelines/<source>.py` calls `ensure_fetched(<pattern>)` at the
top of `main()` so a single command runs URL → disk (with all the
custom fetcher quirks: gov.br F5 bypass, Portal Transparência 404
tolerance, BCB SGS, manifest sha256 idempotence) → disk → Iceberg via
dlt — without duplicating the orchestration that lives in
`lakebrasil.scripts.fetch`.

Pattern matches catalog entry names (fnmatch). Examples:

    ensure_fetched("bacen_*")            # all 3 BACEN series
    ensure_fetched(["ceis", "cnep"])     # both sanction cadastros
    ensure_fetched("camara_*")           # proposições + votações + votos
"""
from __future__ import annotations

import fnmatch
import time
from collections.abc import Iterable

from lakebrasil.fetchers import REGISTRY
from lakebrasil.scripts.catalog import CATALOG_PATH, expand_targets, load_catalog
from lakebrasil.scripts.fetch import RAW_ROOT, _is_fresh


def ensure_fetched(
    pattern: str | Iterable[str],
    *,
    refresh: bool = False,
    raise_on_failure: bool = False,
) -> dict[str, int]:
    """Run fetch for catalog sources matching `pattern` (fnmatch-style).

    Returns counters: {"ok", "skipped", "failed"}. By default, partial
    failures don't abort — Portal Transparência 404 ("not yet
    published") is normal at month boundaries and shouldn't kill the
    load step. Pass `raise_on_failure=True` to harden CI runs.
    """
    sources = load_catalog(CATALOG_PATH)
    patterns = [pattern] if isinstance(pattern, str) else list(pattern)
    selected = {
        n: s for n, s in sources.items()
        if any(fnmatch.fnmatch(n, p) for p in patterns)
    }
    if not selected:
        raise ValueError(
            f"no catalog sources match {patterns!r} — declare them in "
            f"_scripts/catalog.yaml first"
        )

    targets = []
    for spec in selected.values():
        targets.extend(expand_targets(spec, RAW_ROOT))

    counters = {"ok": 0, "skipped": 0, "failed": 0}
    failures: list[tuple[str, str | None]] = []
    print(f"fetch: {len(targets)} targets across {len(selected)} sources "
          f"({', '.join(sorted(selected))})")
    t0 = time.monotonic()

    for i, target in enumerate(targets, 1):
        if not refresh and _is_fresh(target):
            counters["skipped"] += 1
            continue
        fetcher = REGISTRY.get(target.fetcher)
        if fetcher is None:
            counters["failed"] += 1
            failures.append((target.source, f"unknown fetcher {target.fetcher!r}"))
            continue
        result = fetcher(target)
        if result.ok:
            counters["ok"] += 1
            kb = result.bytes_written / 1024
            print(f"  [{i}/{len(targets)}] OK   {target.source:24s} "
                  f"{kb:>10.1f} KB  {target.out_path.name}")
        else:
            counters["failed"] += 1
            failures.append((target.source, result.error))
            print(f"  [{i}/{len(targets)}] FAIL {target.source:24s} {result.error}")

    elapsed = time.monotonic() - t0
    print(f"fetch done in {elapsed:.1f}s — "
          f"ok={counters['ok']} skipped={counters['skipped']} failed={counters['failed']}")

    if raise_on_failure and counters["failed"] > 0:
        raise RuntimeError(
            f"fetch had {counters['failed']} failure(s): {failures[:3]}"
        )
    return counters
