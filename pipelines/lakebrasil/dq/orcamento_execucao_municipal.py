"""DQ: data_br.orcamento_execucao_municipal — despesas federais agregadas."""
from lakebrasil.dq.checks import ibge_coverage, null_rate, row_count, value_range

CHECKS = [
    row_count(min_rows=100_000),  # ~119K rows agregadas reais
    null_rate("mes_competencia"),
    # valor_pago pode ser NEGATIVO (estornos federais — comportamento
    # legítimo do sistema). Sem bound inferior; só sanity superior.
    value_range("valor_pago", max_val=1e12, severity="warn"),
    # Apenas ~3K dos 5571 munis recebem despesa federal direta (resto
    # é via SUS, FUNDEB, etc — não aparece em orcamento_execucao).
    ibge_coverage(min_pct=40, severity="warn"),
]
