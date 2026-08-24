"""GeoServer WFS fetcher — paginates past a hard server-side feature cap.

`terrabrasilis.dpi.inpe.br`'s GeoServer caps `GetFeature` at 50,000
features per response even when the request's `count` param asks for
more (confirmed empirically: `count=1000000` still returns exactly
50,000 with `numberReturned=50000` while `totalFeatures` reports the
real total, e.g. 802,281 for `yearly_deforestation_biome`). A plain
`http` fetch of that URL silently truncates to the cap with no error —
this reassembles the full FeatureCollection via `startIndex` paging.

catalog.yaml entry needs a base WFS 2.0.0 `GetFeature` URL with
`outputFormat=application/json` and NO `count`/`startIndex` — those are
appended per page here. Output is a single combined GeoJSON file
(`.geojson`), not a shapefile zip — no per-attribute type/width limits
to worry about at this feature count.
"""
from __future__ import annotations

import io
import json
import urllib.error
import urllib.request

from .base import (
    FetchResult,
    manifest_key_for,
    s3_key_for_target,
    upload_stream_to_s3,
    write_manifest,
)

PAGE_SIZE = 50_000


def _fetch_page(url: str, start_index: int) -> dict:
    sep = "&" if "?" in url else "?"
    page_url = f"{url}{sep}count={PAGE_SIZE}&startIndex={start_index}"
    req = urllib.request.Request(page_url, headers={"User-Agent": "data-br-fetch/0.1"})
    with urllib.request.urlopen(req, timeout=600) as resp:
        return json.loads(resp.read())


def fetch(target) -> FetchResult:
    s3_key = s3_key_for_target(target)
    try:
        start = 0
        features: list[dict] = []
        crs = None
        pages = 0
        while True:
            page = _fetch_page(target.url, start)
            page_features = page.get("features", [])
            features.extend(page_features)
            crs = page.get("crs", crs)
            returned = page.get("numberReturned", len(page_features))
            total = page.get("totalFeatures")
            start += returned
            pages += 1
            if returned == 0 or (isinstance(total, int) and start >= total):
                break
        combined = {"type": "FeatureCollection", "features": features}
        if crs:
            combined["crs"] = crs
        body = json.dumps(combined).encode("utf-8")
        bytes_w, digest = upload_stream_to_s3(
            io.BytesIO(body), s3_key, content_type="application/geo+json"
        )
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        return FetchResult(False, s3_key, 0, "", target.url, error=str(e))
    except Exception as e:
        return FetchResult(False, s3_key, 0, "", target.url, error=str(e))

    write_manifest(
        manifest_key_for(target.source, s3_key),
        source=target.source,
        url=target.url,
        s3_key=s3_key,
        sha256=digest,
        bytes_written=bytes_w,
        content_type="application/geo+json",
        extra={"params": target.extra, "features": len(features), "pages": pages},
    )
    return FetchResult(True, s3_key, bytes_w, digest, target.url)
