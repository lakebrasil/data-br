"""DQ: data_br.comex_municipio — exportação/importação por município."""
from lakebrasil.dq.checks import (
    fk_to_municipios,
    ibge_coverage,
    null_rate,
    row_count,
    value_range,
)

CHECKS = [
    # 2020-2025 cobre ~16M linhas (~1M/ano por fluxo). Truncation
    # alarme: <5M sugere algo perdido (vimos 8.6M mid-debug, suspeito).
    row_count(min_rows=5_000_000),
    null_rate("ano"),
    null_rate("ibge_code"),
    null_rate("fluxo"),
    value_range("ano", min_val=2020, max_val=2026),
    value_range("mes", min_val=1, max_val=12),
    # Comex é seletivo (só munis com export/import declarados) — ~3K dos
    # 5571. Threshold de 85% era irreal; 50% é o piso esperado.
    ibge_coverage(min_pct=50, severity="warn"),
    fk_to_municipios(),
]
