"""DQ: data_br.macro_serie — séries macro BACEN."""
from lakebrasil.dq.checks import row_count, null_rate, dedup, distinct_count, value_range

CHECKS = [
    row_count(min_rows=10_000),
    null_rate("serie_id"),
    null_rate("data"),
    null_rate("valor"),
    dedup(keys=["serie_id", "data"]),
    distinct_count("serie_id", min_distinct=3),  # ipca, selic, cambio mín
]
