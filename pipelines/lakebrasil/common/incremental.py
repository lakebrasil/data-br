"""Incremental helpers — pipelines reusam pra evitar reprocessar.

Padroniza 4 patterns que estavam duplicados em todo pipeline:
  - `loaded_snapshots(table)` → conjunto de snapshots já carregados
  - `loaded_pairs(table, k1, k2)` → conjunto de tuplas (k1, k2)
  - `loaded_triples(table, k1, k2, k3)` → conjunto de (k1, k2, k3) — granularidade fina
  - `max_value_per(table, group_by, value)` → dict pra incremental por grupo

Todos retornam vazio se a tabela não existe / está sem snapshot —
pipeline vira full-refresh no primeiro run de uma tabela nova.
"""
from __future__ import annotations

from typing import Any

from pyiceberg.exceptions import NoSuchTableError

from lakebrasil.loaders.iceberg import NAMESPACE, catalog


def _scan_arrow(table_name: str, columns: tuple[str, ...], row_filter=None):
    """Tenta carregar arrow restrito às colunas pedidas. None se vazio/n/exist.

    `row_filter` opcional é pushed-down pra pyiceberg → evita full scan
    quando o caller só quer uma fonte específica (essencial em
    indicadores_serie multi-fonte onde a tabela cresce além de 1M rows)."""
    try:
        t = catalog().load_table(f"{NAMESPACE}.{table_name}")
    except NoSuchTableError:
        return None
    if not t.metadata.snapshots:
        return None
    kwargs = {"selected_fields": columns}
    if row_filter is not None:
        kwargs["row_filter"] = row_filter
    return t.scan(**kwargs).to_arrow()


def loaded_snapshots(table_name: str, column: str = "snapshot") -> set[str]:
    """Set de valores distintos de `column` (default 'snapshot') já em
    Iceberg. Use pra pipelines com snapshot único (anp_postos, emendas)."""
    arrow = _scan_arrow(table_name, (column,))
    if arrow is None:
        return set()
    return set(arrow.column(column).to_pylist())


def loaded_pairs(table_name: str, col1: str, col2: str) -> set[tuple[Any, Any]]:
    """Set de (col1, col2) já carregados — pra particionamento composto
    como (cadastro, snapshot) em sancoes ou (programa, mes_competencia)
    em beneficios."""
    arrow = _scan_arrow(table_name, (col1, col2))
    if arrow is None:
        return set()
    return set(zip(
        arrow.column(col1).to_pylist(),
        arrow.column(col2).to_pylist(),
    ))


def loaded_triples(
    table_name: str, col1: str, col2: str, col3: str,
    *,
    fonte: str | None = None,
) -> set[tuple[Any, Any, Any]]:
    """Set de (col1, col2, col3) — granularidade fina pra `indicadores_serie`
    onde queremos pular só combos exatos (fonte, indicador_id, periodo)
    já comitados, em vez de pular a periodo inteira ao bater duas tabelas.

    Use case típico: PAM tabela 1612 ano 2023 commit-ou, mas 1613 ano 2023
    ainda não rodou — `loaded_pairs("...","fonte","periodo")` falsamente
    pularia o ano 2023 inteiro. `loaded_triples` distingue por indicador.

    `fonte` opcional aplica filter push-down em scan — recomendado quando
    o caller só vai checar uma fonte. Sem isso, full table scan vira o
    bottleneck em tabelas multi-fonte com 1M+ rows (PPM no 2º+ run).

    Memória: 1 row por chave distinta. Pra indicadores_serie com 8.9M rows
    × 7 fontes × ~50 indicadores × ~6 periodos ≈ 2K-5K chaves distintas —
    cabe em RAM com folga."""
    row_filter = None
    if fonte is not None:
        from pyiceberg.expressions import EqualTo
        row_filter = EqualTo("fonte", fonte)
    arrow = _scan_arrow(table_name, (col1, col2, col3), row_filter=row_filter)
    if arrow is None:
        return set()
    return set(zip(
        arrow.column(col1).to_pylist(),
        arrow.column(col2).to_pylist(),
        arrow.column(col3).to_pylist(),
    ))


def max_value_per(table_name: str, group_by: str, value: str) -> dict[Any, Any]:
    """`{group_value: max(value)}` por grupo — pra séries temporais
    (bacen.macro_serie: max data por serie_id)."""
    arrow = _scan_arrow(table_name, (group_by, value))
    if arrow is None:
        return {}
    import duckdb
    con = duckdb.connect()
    con.register("t", arrow)
    return {
        r[0]: r[1] for r in con.execute(
            f'SELECT "{group_by}", max("{value}") FROM t GROUP BY "{group_by}"'
        ).fetchall()
    }
