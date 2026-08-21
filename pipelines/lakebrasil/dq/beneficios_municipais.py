"""DQ: data_br.beneficios_municipais — BPC + BF agregados."""
from lakebrasil.dq.checks import (
    dedup,
    distinct_count,
    fk_to_municipios,
    ibge_coverage,
    null_rate,
    row_count,
    value_range,
)

CHECKS = [
    row_count(min_rows=400_000),  # ~580K (350K BPC + 230K BF)
    null_rate("programa"),
    null_rate("mes_competencia"),
    null_rate("uf"),
    distinct_count("programa", min_distinct=2),  # BPC + BOLSA_FAMILIA
    # 63 BPC + 41 BF = 104 (caçar a regressão antiga onde BF caiu pra 25)
    distinct_count("mes_competencia", min_distinct=60, severity="error"),
    value_range("valor_total", min_val=0),
    ibge_coverage(min_pct=99),  # SIAFI map cobre 5569/5571
    fk_to_municipios(severity="warn"),
    dedup(keys=["programa", "mes_competencia", "ibge_code"]),
]
