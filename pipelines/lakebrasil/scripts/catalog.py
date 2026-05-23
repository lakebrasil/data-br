"""catalog.yaml parser + URL expansion.

Loads `_scripts/catalog.yaml` and resolves the param generators into a
flat list of `FetchTarget` (one per concrete URL to download). Used by
`lakebrasil.scripts.fetch` to drive the per-fetcher downloaders.

Generators supported:
    yearly_range:  [start, end]  → 'YYYY' strings, end='today_year' → current year
    monthly_range: [start, end]  → 'YYYY-MM' strings; 'today' → current month
    list:          [...]         → literal list values
    ufs:           true          → all 27 UFs (AC, AL, ..., TO)
"""
from __future__ import annotations

import datetime as _dt
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

CATALOG_PATH = Path(__file__).parent / "catalog.yaml"

UFS = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS",
    "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC",
    "SE", "SP", "TO",
)


@dataclass(frozen=True)
class SourceSpec:
    """Catalog entry for a single source."""

    name: str
    fetcher: str
    schedule: str
    license: str
    out: str
    tier: int
    url: Optional[str] = None
    pattern: Optional[str] = None
    fallback_pattern: Optional[str] = None
    out_filename: Optional[str] = None
    series: Optional[int] = None
    params: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class FetchTarget:
    """One concrete URL to download."""

    source: str
    fetcher: str
    url: str
    out_path: Path
    license: str
    tier: int
    fallback_url: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None  # series id, ym, ano, etc.


def _today_year() -> int:
    return _dt.date.today().year


def _today_ym() -> str:
    d = _dt.date.today()
    return f"{d.year:04d}-{d.month:02d}"


def _parse_ym(s: str) -> _dt.date:
    if s == "today":
        return _dt.date.today().replace(day=1)
    y, m = s.split("-")
    return _dt.date(int(y), int(m), 1)


def _expand_param(name: str, spec: Any) -> List[str]:
    """spec is one of: {yearly_range:[a,b]}, {monthly_range:[a,b]}, {list:[...]}, {ufs:true}."""
    if isinstance(spec, dict):
        if "yearly_range" in spec:
            start, end = spec["yearly_range"]
            end = _today_year() if end == "today_year" else int(end)
            return [str(y) for y in range(int(start), end + 1)]
        if "monthly_range" in spec:
            start, end = spec["monthly_range"]
            d_start = _parse_ym(start)
            d_end = _parse_ym(end)
            out = []
            cur = d_start
            while cur <= d_end:
                out.append(f"{cur.year:04d}-{cur.month:02d}")
                if cur.month == 12:
                    cur = _dt.date(cur.year + 1, 1, 1)
                else:
                    cur = _dt.date(cur.year, cur.month + 1, 1)
            return out
        if "list" in spec:
            return [str(v) for v in spec["list"]]
        if spec.get("ufs") is True:
            return list(UFS)
    raise ValueError(f"unrecognised param spec for {name!r}: {spec!r}")


def _filename_from_url(url: str, out_filename: Optional[str]) -> str:
    if out_filename:
        return out_filename
    # Strip querystring + take last path segment.
    last = url.rsplit("/", 1)[-1].split("?", 1)[0]
    return last or "index.html"


def load_catalog(path: Path = CATALOG_PATH) -> Dict[str, SourceSpec]:
    raw = yaml.safe_load(path.read_text())
    sources: Dict[str, SourceSpec] = {}
    for name, body in raw.get("sources", {}).items():
        sources[name] = SourceSpec(
            name=name,
            fetcher=body["fetcher"],
            schedule=body.get("schedule", "once"),
            license=body.get("license", "unknown"),
            out=body["out"],
            tier=int(body.get("tier", 3)),
            url=body.get("url"),
            pattern=body.get("pattern"),
            fallback_pattern=body.get("fallback_pattern"),
            out_filename=body.get("out_filename"),
            series=body.get("series"),
            params=body.get("params"),
        )
    return sources


def expand_targets(spec: SourceSpec, raw_root: Path) -> Iterable[FetchTarget]:
    """Expand a source spec into one or more concrete fetch targets."""
    out_dir = raw_root / spec.out

    # bcb_sgs is special: there's no URL pattern in catalog — the fetcher
    # builds it from the `series` integer. Emit a single target.
    if spec.fetcher == "bcb_sgs":
        if spec.series is None:
            raise ValueError(f"bcb_sgs source {spec.name} missing `series`")
        yield FetchTarget(
            source=spec.name,
            fetcher=spec.fetcher,
            url=f"sgs://{spec.series}",  # synthetic; resolved by fetcher
            out_path=out_dir / f"{spec.series}_{spec.name.removeprefix('bacen_')}.json",
            license=spec.license,
            tier=spec.tier,
            extra={"series": int(spec.series)},
        )
        return

    # Static URL — single target.
    if spec.url and not spec.pattern:
        yield FetchTarget(
            source=spec.name,
            fetcher=spec.fetcher,
            url=spec.url,
            out_path=out_dir / _filename_from_url(spec.url, spec.out_filename),
            license=spec.license,
            tier=spec.tier,
        )
        return

    # Pattern + params — expand cartesian product.
    if spec.pattern:
        param_lists = {k: _expand_param(k, v) for k, v in (spec.params or {}).items()}
        keys = list(param_lists)
        for combo in itertools.product(*(param_lists[k] for k in keys)):
            ctx = dict(zip(keys, combo))
            url = spec.pattern.format(**ctx)
            fallback = spec.fallback_pattern.format(**ctx) if spec.fallback_pattern else None
            fname = _filename_from_url(url, spec.out_filename)
            yield FetchTarget(
                source=spec.name,
                fetcher=spec.fetcher,
                url=url,
                fallback_url=fallback,
                out_path=out_dir / fname,
                license=spec.license,
                tier=spec.tier,
                extra=ctx,
            )
        return

    raise ValueError(f"source {spec.name} has neither url nor pattern")
