"""DQ: data_br.anp_postos — revendedores varejistas combustíveis."""
from lakebrasil.dq.checks import (
    row_count, null_rate, dedup, ibge_coverage, fk_to_municipios,
)

CHECKS = [
    row_count(min_rows=40_000),  # ~46K hoje
    null_rate("cnpj"),
    null_rate("snapshot"),
    ibge_coverage(min_pct=90, severity="warn"),  # 99.8% via slug match
    fk_to_municipios(severity="warn"),
    dedup(keys=["cnpj", "snapshot"]),
]
