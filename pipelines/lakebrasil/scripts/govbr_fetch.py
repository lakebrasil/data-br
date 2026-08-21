#!/usr/bin/env python3
"""
gov.br F5 BIG-IP firewall bypass.

Many gov.br endpoints (ANP, parts of pdet.mte, others) silently return 403
to curl/wget/httpx because of a TLS-Client-Hello allowlist on the upstream
F5. Python's stdlib `urllib` happens to use a TLS handshake that matches —
so we use it as the canonical downloader for these endpoints.

Usage:
    python3 govbr_fetch.py <url> <output_path>
    python3 govbr_fetch.py --list <urls.txt> <output_dir>
"""
import os
import sys
import urllib.request
from pathlib import Path


def fetch(url: str, output_path: str, chunk_size: int = 1 << 16) -> int:
    """Download URL → output_path. Returns bytes written."""
    req = urllib.request.Request(url, headers={"User-Agent": "Python-urllib/3.13"})
    Path(os.path.dirname(output_path) or ".").mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(req, timeout=300) as response, open(output_path, "wb") as out:
        total = 0
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)
        return total


def main() -> int:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__, file=sys.stderr)
        return 1

    if args[0] == "--list":
        urls_file, out_dir = args[1], args[2]
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        ok = fail = 0
        with open(urls_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Format: <url>\t<filename>  OR  just <url>
                parts = line.split("\t", 1)
                url = parts[0]
                fname = parts[1] if len(parts) > 1 else url.rsplit("/", 1)[-1].split("?")[0]
                out_path = os.path.join(out_dir, fname)
                try:
                    n = fetch(url, out_path)
                    print(f"  OK  {fname}  {n // 1024} KB", flush=True)
                    ok += 1
                except Exception as e:
                    print(f"  FAIL  {fname}  {e}", flush=True)
                    fail += 1
        print(f"\nTotal: {ok} OK, {fail} FAIL")
        return 0 if fail == 0 else 1

    url, output_path = args[0], args[1]
    n = fetch(url, output_path)
    print(f"OK {n // 1024} KB → {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
