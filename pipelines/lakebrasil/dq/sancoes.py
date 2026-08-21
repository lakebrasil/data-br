"""DQ: data_br.sancoes — CEIS + CNEP do Portal da Transparência."""
from lakebrasil.dq.checks import dedup, distinct_count, null_rate, row_count

CHECKS = [
    row_count(min_rows=20_000),  # CEIS ~22K + CNEP ~1.6K
    null_rate("cadastro"),
    null_rate("codigo_sancao"),
    null_rate("snapshot"),
    distinct_count("cadastro", min_distinct=2),  # CEIS + CNEP
    dedup(keys=["cadastro", "codigo_sancao", "snapshot"]),
]
