"""DQ: data_br.empresas_municipio_cnae — CNPJ agregado (ainda vazia)."""
from lakebrasil.dq.checks import row_count

CHECKS = [
    # Schema existe, dados não — pipeline CNPJ ainda não rodou.
    row_count(min_rows=1, severity="warn"),  # warn, não bloqueia
]
