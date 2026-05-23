"""lakebrasil CLI — entry point `lakebrasil`.

Usage:
    lakebrasil list                    # list all available pipelines
    lakebrasil run <pipeline> [args]   # run a pipeline (passes args through)
    lakebrasil dq [table]              # run data quality checks
    lakebrasil fetch <source>          # fetch raw data from gov.br
    lakebrasil version
"""
from __future__ import annotations

import argparse
import importlib
import pkgutil
import sys


def _discover_pipelines() -> list[str]:
    """List all modules in lakebrasil.pipelines (excluding subpackages)."""
    import lakebrasil.pipelines as pkg
    return sorted(
        name for _, name, ispkg in pkgutil.iter_modules(pkg.__path__)
        if not ispkg and not name.startswith("_")
    )


def _cmd_list(_args: argparse.Namespace) -> int:
    pipelines = _discover_pipelines()
    print(f"Available pipelines ({len(pipelines)}):")
    for p in pipelines:
        print(f"  {p}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    name = args.pipeline
    pipelines = _discover_pipelines()
    if name not in pipelines:
        print(f"Pipeline '{name}' not found. Available: {', '.join(pipelines)}",
              file=sys.stderr)
        return 1
    mod = importlib.import_module(f"lakebrasil.pipelines.{name}")
    if not hasattr(mod, "main"):
        print(f"Pipeline '{name}' has no main() function", file=sys.stderr)
        return 1
    # Forward extra args to the pipeline by mutating sys.argv (each pipeline
    # uses argparse internally).
    sys.argv = [f"lakebrasil-{name}", *args.passthrough]
    return mod.main()


def _cmd_dq(args: argparse.Namespace) -> int:
    from lakebrasil.dq.__main__ import main as dq_main
    sys.argv = ["lakebrasil-dq", *([args.table] if args.table else [])]
    return dq_main()


def _cmd_fetch(args: argparse.Namespace) -> int:
    from lakebrasil.scripts.fetch import main as fetch_main
    sys.argv = ["lakebrasil-fetch", "--source", args.source]
    return fetch_main()


def _cmd_version(_args: argparse.Namespace) -> int:
    from lakebrasil import __version__
    print(__version__)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        prog="lakebrasil",
        description="Brazilian public data extraction engine",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="List all available pipelines").set_defaults(func=_cmd_list)
    sub.add_parser("version", help="Show version").set_defaults(func=_cmd_version)

    p_run = sub.add_parser("run", help="Run a pipeline by name")
    p_run.add_argument("pipeline", help="Pipeline name (e.g. bpc, rais, inep)")
    p_run.add_argument("passthrough", nargs=argparse.REMAINDER,
                       help="Arguments forwarded to the pipeline")
    p_run.set_defaults(func=_cmd_run)

    p_dq = sub.add_parser("dq", help="Run data quality checks")
    p_dq.add_argument("table", nargs="?", help="Specific table (default: all)")
    p_dq.set_defaults(func=_cmd_dq)

    p_fetch = sub.add_parser("fetch", help="Fetch raw data from gov source")
    p_fetch.add_argument("source", help="Source name in catalog.yaml")
    p_fetch.set_defaults(func=_cmd_fetch)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
