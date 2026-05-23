"""RAIS via CEMPRE (IBGE Cadastro Central de Empresas) → indicadores_serie.

CEMPRE é o agregado anual municipal derivado da RAIS (Relação Anual de
Informações Sociais, MTE) + outras fontes, publicado pelo IBGE via SIDRA
v3. Estoque de empresas + pessoal ocupado + folha salarial por
município ano-base.

Tabela SIDRA 6449 (Cadastro Central de Empresas) — 4 variáveis × N6
município × ano 2006-2021:

  rais.empresas_total              var 2585 (empresas e organizações)
  rais.pessoal_ocupado_total       var 707  (pessoas)
  rais.pessoal_ocupado_assalariado var 708  (pessoas)
  rais.salarios_total_mil_reais    var 662  (R$ mil)

Filtra CNAE = 117897 (Total) — sem detalhe setorial. Para detalhe por
CNAE existem outras tabelas (futuro).

1 SIDRA call por (variavel, ano) = 4 × 16 = 64 calls, cada call cobre
~5.500 munis. ThreadPoolExecutor(6) paraleliza pra ~3-5min total.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.rais --no-fetch
"""
from __future__ import annotations

import argparse
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.pipelines.pam import _http_get_json, API_BASE
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

TABELA = 6449
CLASSIF = 12762   # CNAE 2.0
CAT_TOTAL = "117897"

# (var_id, suffix, unidade)
VARS = [
    ("2585", "empresas_total",              "empresas"),
    ("707",  "pessoal_ocupado_total",       "pessoas"),
    ("708",  "pessoal_ocupado_assalariado", "pessoas"),
    ("662",  "salarios_total_mil_reais",    "R$ mil"),
]

SIDRA_WORKERS = 6


def _build_ibge_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    return {int(i): u or "??" for i, u in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
    ) if i is not None}


def _fetch_cempre(ano: int, var: str, suffix: str, unidade: str,
                   ibge_to_uf: dict[int, str]) -> list[dict]:
    classif_p = urllib.parse.quote(f"{CLASSIF}[{CAT_TOTAL}]", safe="")
    loc_p = urllib.parse.quote("N6[all]", safe="")
    url = (f"{API_BASE}/{TABELA}/periodos/{ano}/variaveis/{var}"
           f"?localidades={loc_p}&classificacao={classif_p}")
    data = _http_get_json(url)
    out: list[dict] = []
    for v in data:
        for r in v.get("resultados", []):
            for s in r.get("series", []):
                loc = s.get("localidade", {})
                ibge_raw = loc.get("id")
                if not ibge_raw:
                    continue
                try:
                    ibge = int(ibge_raw)
                except (ValueError, TypeError):
                    continue
                if ibge not in ibge_to_uf:
                    continue
                uf = ibge_to_uf.get(ibge, "??")
                serie = s.get("serie", {})
                for periodo, raw in serie.items():
                    if raw in ("..", "...", "-", None, "", "X"):
                        continue
                    try:
                        valor = float(raw)
                    except (ValueError, TypeError):
                        continue
                    out.append({
                        "ibge_code": ibge, "uf": uf,
                        "fonte": "rais",
                        "indicador_id": f"rais.{suffix}",
                        "periodo": str(periodo),
                        "valor": valor,
                        "valor_texto": None,
                        "unidade": unidade,
                        "fonte_arquivo": f"sidra_api:{TABELA}",
                    })
    return out


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ano", type=int, action="append",
                   help="Anos a carregar (default: 2006-2021).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    if not args.no_fetch:
        try:
            ensure_fetched("rais_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] sem catalog entry rais — SIDRA direto.",
                  file=sys.stderr)

    ibge_to_uf = _build_ibge_to_uf()
    print(f"RAIS→ibge dim: {len(ibge_to_uf):,} munis", file=sys.stderr)

    # Descobre periodos via SIDRA se --ano não passado
    if args.ano:
        anos = sorted(set(args.ano))
    else:
        periodos = _http_get_json(f"{API_BASE}/{TABELA}/periodos")
        anos = sorted(int(p["id"]) for p in periodos)
    print(f"RAIS: {len(anos)} anos × {len(VARS)} vars = {len(anos)*len(VARS)} SIDRA calls",
          file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="rais")

    # Filtra (ano, var) já carregados
    jobs = []
    for ano in anos:
        for var, suffix, unidade in VARS:
            if ("rais", f"rais.{suffix}", str(ano)) in already:
                continue
            jobs.append((ano, var, suffix, unidade))
    print(f"RAIS pendentes: {len(jobs)} jobs (workers={SIDRA_WORKERS})",
          file=sys.stderr)

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def rais_indicadores() -> Iterator[dict]:
        done = 0
        with ThreadPoolExecutor(max_workers=SIDRA_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_cempre, ano, var, suffix, unidade,
                            ibge_to_uf): (ano, var)
                for ano, var, suffix, unidade in jobs
            }
            for fut in as_completed(futures):
                done += 1
                if done % 8 == 0 or done == len(jobs):
                    print(f"    RAIS: {done}/{len(jobs)} jobs", file=sys.stderr)
                yield from fut.result()

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="rais_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(rais_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, periodo, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id, periodo "
                "ORDER BY periodo DESC, indicador_id LIMIT 20"))
        return 0

    pipe = dlt.pipeline(pipeline_name="rais", destination=s3tables_iceberg)
    info = pipe.run(rais_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
