"""DQ: data_br.emendas_parlamentares."""
from lakebrasil.dq.checks import (
    dedup,
    fk_to_municipios,
    null_rate,
    row_count,
    value_range,
)

CHECKS = [
    row_count(min_rows=80_000),  # ~92K hoje
    null_rate("codigo_emenda"),
    null_rate("snapshot"),
    value_range("ano_emenda", min_val=2014, max_val=2026),
    fk_to_municipios(severity="warn"),  # ~40% têm ibge (resto = "Sem informação")
    # Mesmo codigo_emenda + (funcao, subfuncao, programa, acao, ibge)
    # aparece múltiplas vezes — uma emenda é executada em vários
    # momentos com valores diferentes. Sem campo `id_execucao` no
    # dump do Portal, não dá pra ter chave naturalmente única.
    # Mantemos como warn (info) — re-runs geram dups novos sem o
    # incremental, mas o snapshot único atual + run idempotente os
    # evita na prática.
    dedup(keys=["codigo_emenda", "codigo_funcao", "codigo_subfuncao",
                "codigo_programa", "codigo_acao", "ibge_code"],
          severity="warn"),
]
