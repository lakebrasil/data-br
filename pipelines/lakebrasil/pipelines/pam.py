"""IBGE PAM — Produção Agrícola Municipal → data_br.indicadores_serie.

Fetch + load direto via SIDRA API v3 (em vez de depender de raw files
broken). Os JSONs em `s3://data-br-raw/pam/raw/pam_temp_*.json` foram
upload externo com filtro inválido (sem `classificacao=81[all]`),
resultando em 100% valores '..'. Esta pipeline ignora o raw e busca
direto da API.

Endpoint: servicodados.ibge.gov.br/api/v3/agregados/{tabela}/...
  tabela 1612 = Lavouras temporárias (cana, soja, milho, mandioca, ...)
  tabela 1613 = Lavouras permanentes (café, cacau, citros, ...)

Para evitar o limit 50K valores/call, fazemos 1 chamada por
(tabela, ano, variavel, produto) = ~5570 munis × 1 valor = ~5K cabe.

Variáveis principais (mesmas pra ambas tabelas):
  214 = Quantidade produzida (Toneladas)
  216 = Área plantada (Hectares)
  112 = Área colhida (Hectares)
  215 = Rendimento médio (kg/hectare)
  214? = Valor da produção (Mil reais)  -- fica TODO

Output em indicadores_serie:
  ibge_code     ← localidade.id
  fonte         ← 'pam'
  indicador_id  ← `pam.{tabela}.{variavel}.{produto_id}` (e.g. pam.1612.214.2713 = soja qtd)
  periodo       ← '{ano}' (YYYY)
  valor         ← float(serie.{ano}) — skipa '..', '-', etc
  valor_texto   ← null
  unidade       ← variavel.unidade
  fonte_arquivo ← 'sidra_api:1612'

Source: sidra.ibge.gov.br/pesquisa/pam/tabelas

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.pam --no-fetch
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.pam --ano 2023 --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Iterator

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

# (tabela_id, friendly_name, classificacao_id)
TABELAS = [
    (1612, "Lavouras temporárias", 81),
    (1613, "Lavouras permanentes", 82),
]
# 4 variáveis-chave. Códigos consistentes entre 1612 e 1613.
VARIAVEIS = ("214", "216", "112", "215")

API_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados"


def _http_get_json(url: str, retries: int = 3, backoff: float = 2.0):
    """GET + JSON parse com retry exponencial em 5xx/timeout.

    SIDRA pode responder gzip mesmo sem Accept-Encoding gzip — desabilita
    explicitamente via `identity` pra evitar UTF-8 decode error em 0x8b.
    """
    import gzip
    last: Exception | None = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "data-br/pam",
                "Accept-Encoding": "identity",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read()
                # Defesa: se ainda veio gzip, decompacta.
                if body[:2] == b"\x1f\x8b":
                    body = gzip.decompress(body)
                return json.loads(body)
        except Exception as e:
            last = e
            if i < retries - 1:
                time.sleep(backoff * (i + 1))
    raise RuntimeError(f"SIDRA fetch failed after {retries} retries: {last}")


def _get_produtos(tabela: int, classif: int) -> list[tuple[str, str]]:
    """Retorna [(produto_id, nome)] da classificacao. Skipa '0' (Total)."""
    url = f"{API_BASE}/{tabela}/metadados"
    meta = _http_get_json(url)
    for c in meta.get("classificacoes", []):
        if c.get("id") == classif:
            return [
                (str(p["id"]), p["nome"]) for p in c.get("categorias", [])
                if str(p["id"]) != "0"
            ]
    return []


def _iter_sidra(tabela: int, ano: int, variavel: str,
                produto_id: str, classif: int) -> Iterator[dict]:
    """Faz 1 chamada SIDRA → yield rows pré-formatadas pra indicadores_serie."""
    classif_param = urllib.parse.quote(f"{classif}[{produto_id}]", safe="")
    loc_param = urllib.parse.quote("N6[all]", safe="")
    url = (f"{API_BASE}/{tabela}/periodos/{ano}/variaveis/{variavel}"
           f"?localidades={loc_param}&classificacao={classif_param}")
    data = _http_get_json(url)
    for v in data:
        unidade = v.get("unidade")
        for r in v.get("resultados", []):
            for s in r.get("series", []):
                loc = s.get("localidade", {})
                ibge = loc.get("id")
                if not ibge or loc.get("nivel", {}).get("id") != "N6":
                    continue
                try:
                    ibge_int = int(ibge)
                except (ValueError, TypeError):
                    continue
                uf = (loc.get("nome", "").rsplit("(", 1)[-1].rstrip(")").strip()
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
                        "fonte":         "pam",
                        "indicador_id":  f"pam.{tabela}.{variavel}.{produto_id}",
                        "periodo":       str(periodo),
                        "valor":         valor,
                        "valor_texto":   None,
                        "unidade":       unidade,
                        "fonte_arquivo": f"sidra_api:{tabela}",
                    }


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ano", type=int, action="append",
                   help="Anos a carregar (default: 2020-2024).")
    p.add_argument("--tabela", type=int, choices=[1612, 1613],
                   help="Só 1 tabela (1612=temporárias, 1613=permanentes).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()
    anos = args.ano or list(range(2020, 2025))
    tabelas = [(t, n, c) for (t, n, c) in TABELAS if not args.tabela or t == args.tabela]

    def _resource_for(tab_id: int, classif: int, produtos: list[tuple[str, str]],
                      ano: int, already_triples: set):
        """Resource dlt pra UMA (tabela, ano) — emite tudo em 1 run() pra
        que dlt commit fique granular. Evita perda total se interrompido.

        `already_triples` filtra por (fonte, indicador_id, periodo) —
        granularidade fina permite re-run pular SÓ produtos+variáveis já
        comitados dentro da (tabela, ano), em vez do periodo inteiro."""
        @dlt.resource(
            name="indicadores_serie",
            primary_key=["ibge_code", "indicador_id", "periodo"],
            write_disposition="append",
        )
        def _r() -> Iterator[dict]:
            # Paraleliza calls SIDRA via ThreadPoolExecutor — cada call é
            # I/O bound (20-40s SIDRA round-trip). 6 workers ≈ 5x speedup
            # sem 429s (testado em PPM).
            from concurrent.futures import ThreadPoolExecutor, as_completed
            jobs = []
            for var in VARIAVEIS:
                for prod_id, _ in produtos:
                    indicador = f"pam.{tab_id}.{var}.{prod_id}"
                    if ("pam", indicador, str(ano)) in already_triples:
                        continue
                    jobs.append((var, prod_id))
            if not jobs:
                return
            done = 0
            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = {
                    pool.submit(
                        lambda v, p: list(_iter_sidra(tab_id, ano, v, p, classif)),
                        var, prod_id,
                    ): (var, prod_id)
                    for var, prod_id in jobs
                }
                for fut in as_completed(futures):
                    done += 1
                    if done % 10 == 0 or done == len(jobs):
                        print(f"    PAM tab={tab_id} ano={ano}: {done}/{len(jobs)} jobs",
                              file=sys.stderr)
                    yield from fut.result()
        return _r

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="pam_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        # dry-run só com 1 ano + 1 tabela
        ano = anos[0]
        tab_id, _tab, classif = tabelas[0]
        produtos = _get_produtos(tab_id, classif)
        pipe.run(_resource_for(tab_id, classif, produtos, ano, set())())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), sum(valor) AS total "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC LIMIT 15"))
        return 0

    # Commit granular: 1 pipe.run() por (tabela, ano). Cada run commita
    # ~140K rows ao Iceberg em ~12s. Se mata-se mid-execução, perde só
    # os produtos não-comitados (filtered via loaded_triples no resource).
    pipe = dlt.pipeline(pipeline_name="pam", destination=s3tables_iceberg)
    for tab_id, tab_nome, classif in tabelas:
        print(f"  PAM tabela {tab_id} ({tab_nome}): listando produtos...",
              file=sys.stderr)
        produtos = _get_produtos(tab_id, classif)
        print(f"  PAM tabela {tab_id}: {len(produtos)} produtos",
              file=sys.stderr)
        for ano in anos:
            # Granularidade fina: filtra por (fonte, indicador_id, periodo).
            # Permite resume EM MEIO de uma (tabela, ano) — só os produtos
            # não-emitidos rodam, os já comitados pulam.
            already_now = set() if args.full_refresh else loaded_triples(
                "indicadores_serie", "fonte", "indicador_id", "periodo",
                fonte="pam")
            # Optimization: se TODOS os indicadores dessa (tab, ano) já
            # estão em already, pula a (tab, ano) inteira (skip API meta call).
            todos_indicadores = {
                f"pam.{tab_id}.{var}.{prod_id}"
                for var in VARIAVEIS
                for (prod_id, _) in produtos
            }
            esperados = {("pam", ind, str(ano)) for ind in todos_indicadores}
            faltam = esperados - already_now
            if not faltam:
                print(f"  PAM tab={tab_id} ano={ano}: skip (todos indicadores já loaded)",
                      file=sys.stderr)
                continue
            t0 = time.monotonic()
            print(f"  PAM tab={tab_id} ano={ano}: rodando "
                  f"{len(faltam)}/{len(esperados)} indicadores", file=sys.stderr)
            res = _resource_for(tab_id, classif, produtos, ano, already_now)
            info = pipe.run(res())
            print(f"  PAM tab={tab_id} ano={ano}: committed em "
                  f"{time.monotonic()-t0:.1f}s — {info}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
