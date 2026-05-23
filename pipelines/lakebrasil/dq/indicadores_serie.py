"""DQ: data_br.indicadores_serie — fato genérica (SNIS, ANA, etc)."""
from lakebrasil.dq.checks import (
    row_count, null_rate, distinct_count, ibge_coverage, fk_to_municipios,
)

CHECKS = [
    # SNIS ainda não foi reloaded pra Iceberg (load original perdeu).
    # Quando entrar, total esperado ~172K (84K ANA + 88K SNIS).
    row_count(min_rows=80_000, severity="warn"),
    null_rate("ibge_code"),
    null_rate("indicador_id"),
    null_rate("periodo"),
    null_rate("fonte"),
    distinct_count("fonte", min_distinct=2, severity="warn"),  # snis + ana
    ibge_coverage(min_pct=80),
    fk_to_municipios(),
]
