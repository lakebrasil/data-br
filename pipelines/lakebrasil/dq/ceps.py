"""DQ: data_br.ceps — universo postal BR."""
from lakebrasil.dq.checks import row_count, null_rate, dedup, distinct_count

CHECKS = [
    row_count(min_rows=1_000_000),  # universo BR ~1.27M CEPs
    null_rate("cep"),
    null_rate("uf"),
    dedup(keys=["cep"]),
    distinct_count("uf", min_distinct=27),
]
