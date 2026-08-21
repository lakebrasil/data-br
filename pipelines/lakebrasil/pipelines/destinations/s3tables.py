"""Backward-compat re-export. Old name (`s3tables_iceberg`) → new pluggable
`iceberg` destination. New code should use `lakebrasil.pipelines.destinations.iceberg`."""
from lakebrasil.pipelines.destinations.iceberg import iceberg as s3tables_iceberg

__all__ = ["s3tables_iceberg"]
