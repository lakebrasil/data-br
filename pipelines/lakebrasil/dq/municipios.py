"""DQ: data_br.municipios — dim universal."""
from lakebrasil.dq.checks import row_count, null_rate, dedup, distinct_count

CHECKS = [
    row_count(min_rows=5571, max_rows=5571),  # IBGE 2022 census, exato
    null_rate("ibge_code"),
    null_rate("name"),
    null_rate("uf"),
    dedup(keys=["ibge_code"]),
    distinct_count("uf", min_distinct=27),  # 26 estados + DF
]
