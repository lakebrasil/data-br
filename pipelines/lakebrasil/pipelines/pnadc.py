"""IBGE PNAD Contínua → indicadores_serie (apenas 27 capitais).

PNAD Contínua é pesquisa amostral domiciliar do IBGE — só publica
indicadores por município pras 27 capitais (resto da amostra cobre só
UF/RM/região). Cobertura municipal limitada mas series trimestrais
2012Q1 → atual em ~60 períodos.

Tabela SIDRA 4093 (Pessoas 14+ por força trabalho) — 11 indicadores
× ~60 trimestres × 27 capitais.

Variáveis emitidas:
  pnadc.pessoas_14_mais            var 1641 (mil pessoas)
  pnadc.forca_trabalho             var 4088
  pnadc.ocupados                   var 4090
  pnadc.desocupados                var 4092
  pnadc.fora_forca_trabalho        var 4094
  pnadc.taxa_participacao_pct      var 4096 (%)
  pnadc.nivel_ocupacao_pct         var 4097
  pnadc.nivel_desocupacao_pct      var 4098
  pnadc.taxa_desocupacao_pct       var 4099
  pnadc.taxa_informalidade_pct     var 12466
  pnadc.ocupados_informais         var 4723

Periodo = trimestre YYYYMM (ex: 202403 = 2024 Q3).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.pnadc --no-fetch
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

TABELA = 4093

VARS = [
    ("1641",  "pessoas_14_mais",        "mil pessoas"),
    ("4088",  "forca_trabalho",         "mil pessoas"),
    ("4090",  "ocupados",               "mil pessoas"),
    ("4092",  "desocupados",            "mil pessoas"),
    ("4094",  "fora_forca_trabalho",    "mil pessoas"),
    ("4096",  "taxa_participacao_pct",  "%"),
    ("4097",  "nivel_ocupacao_pct",     "%"),
    ("4098",  "nivel_desocupacao_pct",  "%"),
    ("4099",  "taxa_desocupacao_pct",   "%"),
    ("12466", "taxa_informalidade_pct", "%"),
    ("4723",  "ocupados_informais",     "mil pessoas"),
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


def _fetch(periodo: str, var: str, suffix: str, unidade: str,
           ibge_to_uf: dict[int, str]) -> list[dict]:
    loc_p = urllib.parse.quote("N6[all]", safe="")
    url = f"{API_BASE}/{TABELA}/periodos/{periodo}/variaveis/{var}?localidades={loc_p}"
    try:
        data = _http_get_json(url)
    except Exception as e:
        # SIDRA fica intermitente — tolera falhas e re-tenta no próximo run
        print(f"  PNADc skip {periodo}/{var}: {e}", file=sys.stderr)
        return []
    out: list[dict] = []
    for v in data:
        for r in v.get("resultados", []):
            for s in r.get("series", []):
                loc = s.get("localidade", {})
                if loc.get("nivel", {}).get("id") != "N6":
                    continue
                try:
                    ibge = int(loc.get("id", "0"))
                except (ValueError, TypeError):
                    continue
                if ibge not in ibge_to_uf:
                    continue
                uf = ibge_to_uf[ibge]
                serie = s.get("serie", {})
                for per, raw in serie.items():
                    if raw in ("..", "...", "-", None, "", "X"):
                        continue
                    try:
                        valor = float(raw)
                    except (ValueError, TypeError):
                        continue
                    out.append({
                        "ibge_code": ibge, "uf": uf,
                        "fonte": "pnadc",
                        "indicador_id": f"pnadc.{suffix}",
                        "periodo": str(per),
                        "valor": valor,
                        "valor_texto": None,
                        "unidade": unidade,
                        "fonte_arquivo": f"sidra_api:{TABELA}",
                    })
    return out


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--periodo", action="append",
                   help="Trimestres YYYYMM. Default: todos disponíveis.")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    if not args.no_fetch:
        try:
            ensure_fetched("pnadc_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] sem catalog entry pnadc — SIDRA direto.",
                  file=sys.stderr)

    ibge_to_uf = _build_ibge_to_uf()
    print(f"PNADc→ibge dim: {len(ibge_to_uf):,} munis", file=sys.stderr)

    if args.periodo:
        periodos = sorted(set(args.periodo))
    else:
        # Hard-coded 2012Q1 → 2026Q1 (SIDRA /periodos endpoint é instável).
        # PNADc v3 começou em 2012; novos trimestres saem ~50 dias após
        # fim do tri — manter este range bumpado periodicamente.
        periodos = [f"{y}{q:02d}" for y in range(2012, 2027) for q in range(1, 5)]
        # Trim past current quarter
        from datetime import date
        today = date.today()
        cur_q = ((today.month - 1) // 3) + 1
        max_per = f"{today.year}{cur_q:02d}"
        periodos = [p for p in periodos if p <= max_per]
    print(f"PNADc: {len(periodos)} trimestres × {len(VARS)} vars = "
          f"{len(periodos)*len(VARS)} SIDRA calls", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo", fonte="pnadc")

    jobs = []
    for per in periodos:
        for var, suffix, unidade in VARS:
            if ("pnadc", f"pnadc.{suffix}", per) in already:
                continue
            jobs.append((per, var, suffix, unidade))
    print(f"PNADc pendentes: {len(jobs)} jobs (workers={SIDRA_WORKERS})",
          file=sys.stderr)

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def pnadc_indicadores() -> Iterator[dict]:
        done = 0
        with ThreadPoolExecutor(max_workers=SIDRA_WORKERS) as pool:
            futures = {
                pool.submit(_fetch, per, var, suffix, unidade, ibge_to_uf):
                    (per, var)
                for per, var, suffix, unidade in jobs
            }
            for fut in as_completed(futures):
                done += 1
                if done % 50 == 0 or done == len(jobs):
                    print(f"    PNADc: {done}/{len(jobs)}", file=sys.stderr)
                yield from fut.result()

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="pnadc_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(pnadc_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), avg(valor) "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC LIMIT 15"))
        return 0

    pipe = dlt.pipeline(pipeline_name="pnadc", destination=s3tables_iceberg)
    info = pipe.run(pnadc_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
