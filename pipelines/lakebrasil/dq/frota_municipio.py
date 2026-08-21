"""DQ: data_br.frota_municipio — DENATRAN frota."""
from lakebrasil.dq.checks import fk_to_municipios, ibge_coverage, null_rate, row_count

CHECKS = [
    row_count(min_rows=100_000),
    null_rate("ibge_code"),
    null_rate("periodo"),
    ibge_coverage(min_pct=99),  # universo municipal completo
    fk_to_municipios(),
]
