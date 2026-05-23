"""DQ: data_br.frota_municipio — DENATRAN frota."""
from lakebrasil.dq.checks import row_count, null_rate, ibge_coverage, fk_to_municipios

CHECKS = [
    row_count(min_rows=100_000),
    null_rate("ibge_code"),
    null_rate("periodo"),
    ibge_coverage(min_pct=99),  # universo municipal completo
    fk_to_municipios(),
]
