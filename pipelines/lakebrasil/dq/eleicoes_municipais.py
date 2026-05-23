"""DQ: data_br.eleicoes_municipais — TSE candidatos."""
from lakebrasil.dq.checks import (
    row_count, null_rate, value_range, ibge_coverage, fk_to_municipios, dedup,
)

CHECKS = [
    row_count(min_rows=1_000_000),
    null_rate("ano"),
    null_rate("ibge_code"),
    null_rate("cargo"),
    value_range("ano", min_val=2008, max_val=2026),
    ibge_coverage(min_pct=99),
    fk_to_municipios(),
    dedup(keys=["ano", "turno", "sq_candidato", "cargo"]),
]
