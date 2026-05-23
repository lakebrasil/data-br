"""IBGE PPM — Pesquisa Pecuária Municipal → data_br.indicadores_serie.

Fetch + load via SIDRA API v3 (mesmo padrão do _pipelines/pam.py).
Cobre:

  Tabela 3939 — Efetivo dos rebanhos (cabeças por tipo: bovino, suíno,
                ovino, caprino, galináceos, equino, bubalino, etc)
                variável 105, classificação 79[2670, 2675, ...]
  Tabela 74   — Produção de leite (litros/ano por município, sem classif)
                variável 106
  Tabela 94   — Produção de mel (kg/ano por município, sem classif)
                variável 106
  Tabela 95   — Produção de ovos de galinha (mil dúzias/ano, sem classif)
                variáveis 29 + 105
  Tabela 915  — Produção de lã (kg/ano por município, sem classif)
                variável 106

Tables com classificação → 1 call per (tabela, ano, var, produto_id).
Tables sem classificação → 1 call per (tabela, ano, var) com all munis
(cabe no limit 50K — 5570 munis × 1 valor ~ 5K).

Output em indicadores_serie:
  ibge_code     ← localidade.id
  fonte         ← 'ppm'
  indicador_id  ← `ppm.{tabela}.{variavel}` ou `ppm.{tabela}.{variavel}.{produto_id}`
                  ex: ppm.3939.105.2670 = bovino cabeças
                      ppm.74.106          = leite produção
  periodo       ← '{ano}' (YYYY)
  valor         ← float(serie.{ano})
  unidade       ← variavel.unidade

Source: sidra.ibge.gov.br/pesquisa/ppm.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.ppm --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.ppm --ano 2023 --tabela 3939
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

# Reusa helpers do PAM pipeline pra não duplicar (API client + parser).
from lakebrasil.pipelines.pam import _get_produtos, _http_get_json, API_BASE
from lakebrasil.pipelines.pam import _iter_sidra as _iter_sidra_classif

# Paralelismo SIDRA. 6 workers ainda cabe no rate limit (sem 429s
# observados) e dá ~5x speedup vs serial em tabelas com muitos produtos.
SIDRA_WORKERS = 6

# (tabela_id, friendly, classif_id ou None, variaveis_list)
TABELAS: list[tuple[int, str, int | None, list[str]]] = [
    (3939, "Efetivo dos rebanhos",  79,   ["105"]),    # 1 var × 16+ tipos rebanho
    (74,   "Produção origem animal",80,   ["106"]),    # 1 var × 6 produtos (leite/ovos/mel/lã)
    (94,   "Vacas ordenhadas",      None, ["107"]),    # cabeças/ano (sem classif)
    (95,   "Ovinos tosquiados",     None, ["108"]),    # cabeças/ano (sem classif)
    # 915 (Produção ovos galinha) é série encerrada — sem dados recentes; skipado.
]


def _iter_sidra_no_classif(tabela: int, ano: int, variavel: str) -> Iterator[dict]:
    """1 chamada SIDRA sem filtro de classificação (tabela tem 1 dim só)."""
    import urllib.parse
    loc = urllib.parse.quote("N6[all]", safe="")
    url = (f"{API_BASE}/{tabela}/periodos/{ano}/variaveis/{variavel}"
           f"?localidades={loc}")
    data = _http_get_json(url)
    for v in data:
        unidade = v.get("unidade")
        for r in v.get("resultados", []):
            for s in r.get("series", []):
                loc_obj = s.get("localidade", {})
                ibge = loc_obj.get("id")
                if not ibge or loc_obj.get("nivel", {}).get("id") != "N6":
                    continue
                try:
                    ibge_int = int(ibge)
                except (ValueError, TypeError):
                    continue
                uf = (loc_obj.get("nome", "").rsplit("(", 1)[-1].rstrip(")").strip()
                      or None)
                serie = s.get("serie", {})
                for periodo, raw in serie.items():
                    if raw in ("..", "...", "-", None, "", "X"):
                        continue
                    try:
                        valor = float(raw)
                    except (ValueError, TypeError):
                        continue
                    yield {
                        "ibge_code":     ibge_int,
                        "uf":            uf,
                        "fonte":         "ppm",
                        "indicador_id":  f"ppm.{tabela}.{variavel}",
                        "periodo":       str(periodo),
                        "valor":         valor,
                        "valor_texto":   None,
                        "unidade":       unidade,
                        "fonte_arquivo": f"sidra_api:{tabela}",
                    }


def _fetch_classif_batch(tabela: int, ano: int, var: str, prod_id: str,
                          classif: int) -> list[dict]:
    """Drena 1 (var, produto) inteiro pra uma lista — chamável em ThreadPool."""
    out = []
    for rec in _iter_sidra_classif(tabela, ano, var, prod_id, classif):
        rec["fonte"] = "ppm"
        rec["indicador_id"] = f"ppm.{tabela}.{var}.{prod_id}"
        rec["fonte_arquivo"] = f"sidra_api:{tabela}"
        out.append(rec)
    return out


def _iter_table(tabela: int, classif: int | None, variaveis: list[str],
                ano: int) -> Iterator[dict]:
    """Yield rows pra uma tabela inteira em um ano.

    Para tabelas com classificação, paraleliza N produtos × M variáveis
    em ThreadPoolExecutor(SIDRA_WORKERS) — cada call SIDRA leva 20-40s
    e são I/O bound, então threads são suficientes (sem GIL contention).
    """
    if classif is None:
        for var in variaveis:
            yield from _iter_sidra_no_classif(tabela, ano, var)
            time.sleep(0.05)
        return

    produtos = _get_produtos(tabela, classif)
    print(f"  PPM tab={tabela}: {len(produtos)} produtos classif {classif} "
          f"× {len(variaveis)} vars (workers={SIDRA_WORKERS})", file=sys.stderr)
    jobs = [(var, prod_id) for var in variaveis for prod_id, _ in produtos]
    done = 0
    with ThreadPoolExecutor(max_workers=SIDRA_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_classif_batch, tabela, ano, var, prod_id, classif):
                (var, prod_id)
            for var, prod_id in jobs
        }
        for fut in as_completed(futures):
            done += 1
            if done % 3 == 0 or done == len(jobs):
                print(f"    PPM tab={tabela} ano={ano}: {done}/{len(jobs)} jobs",
                      file=sys.stderr)
            for rec in fut.result():
                yield rec


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ano", type=int, action="append",
                   help="Anos a carregar (default: 2020-2024).")
    p.add_argument("--tabela", type=int,
                   help="Só 1 tabela (3939/74/94/95/915).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def _resource_for_ppm(tab_id: int, classif: int | None, variaveis: list[str],
                      ano: int, already_triples: set):
    """Resource pra UMA (tabela, ano) PPM. Commit granular via per-iteration
    pipe.run(). Filtra por (fonte, indicador_id, periodo) — resume fino."""
    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def _r() -> Iterator[dict]:
        for rec in _iter_table(tab_id, classif, variaveis, ano):
            if ("ppm", rec["indicador_id"], rec["periodo"]) in already_triples:
                continue
            yield rec
    return _r


def main() -> int:
    args = _build_args()
    anos = args.ano or list(range(2020, 2025))
    tabelas = [t for t in TABELAS if not args.tabela or t[0] == args.tabela]

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="ppm_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        tab_id, _tab, classif, variaveis = tabelas[0]
        pipe.run(_resource_for_ppm(tab_id, classif, variaveis, anos[0], set())())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), sum(valor) AS total "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC LIMIT 15"))
        return 0

    # Commit granular: 1 pipe.run() por (tabela, ano). Resume fino via
    # loaded_triples — re-run só processa (indicador_id, periodo) não
    # comitados, ao invés de pular o periodo inteiro.
    pipe = dlt.pipeline(pipeline_name="ppm", destination=s3tables_iceberg)
    for tab_id, tab_nome, classif, variaveis in tabelas:
        print(f"  PPM tabela {tab_id} ({tab_nome}): {len(variaveis)} variáveis",
              file=sys.stderr)
        for ano in anos:
            already_now = set() if args.full_refresh else loaded_triples(
                "indicadores_serie", "fonte", "indicador_id", "periodo",
                fonte="ppm")
            t0 = time.monotonic()
            print(f"  PPM tab={tab_id} ano={ano}: rodando", file=sys.stderr)
            res = _resource_for_ppm(tab_id, classif, variaveis, ano, already_now)
            info = pipe.run(res())
            print(f"  PPM tab={tab_id} ano={ano}: committed em "
                  f"{time.monotonic()-t0:.1f}s — {info}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
