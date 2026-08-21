"""SNIS — Sistema Nacional Informações Saneamento → data_br.indicadores_serie.

Carrega `s3://data-br-raw/snis/raw/Planilhas_AE{YYYY}.zip` (Diagnóstico
dos Serviços de Água e Esgotos) e extrai indicadores de saneamento
por município:

  snis.IN055  Índice de atendimento total de água        (%)
  snis.IN023  Índice de atendimento urbano de água       (%)
  snis.IN056  Índice de atendimento total de esgoto      (%)
  snis.IN015  Índice de coleta de esgoto                 (%)
  snis.IN016  Índice de tratamento de esgoto             (%)
  snis.IN049  Índice de perdas na distribuição           (%)

Os 5 primeiros destravam colunas que estavam 100% NULL em
`data_br.municipios` (agua_atendimento_pct, esgoto_atendimento_pct,
esgoto_tratamento_pct, perda_agua_pct) — agora disponíveis via
JOIN com indicadores_serie.

Estrutura do raw:
  Planilhas_AE{YYYY}.zip
    └── Planilhas_AE{YYYY}/
          └── Planilha_AE{YYYY}_Completa_LPU.zip   (urbana — single)
                └── Planilha_AE{YYYY}_Completa_LPU/
                      ├── Planilha_LPU_Indicadores.xls  ← ESTE
                      └── Planilha_LPU_Informacoes.xls

Combina LPU (autarquias municipais) + LPR (locais privados) + LEP
(empresas privadas) + Regionais (28 CESBs estaduais — Sabesp, Copasa,
Sanepar, etc.) — cobertura ~80% dos 5.571 municípios.

Código SNIS = IBGE 6-dígitos (truncado sem dv). Crosswalk para IBGE
7-dig via `data_br.municipios.ibge_code // 10 == co6`.

Cobertura temporal: 2020-2022. SNIS foi descontinuado em 2023; AE2022
(publicado em 2023) foi a última edição. Dados 2023+ ficam no sistema
sucessor SINISA (Sistema Nacional de Informações sobre Saneamento
Básico), com schema diferente (SAA/SES/SDU/SMU em vez de IN001-IN085)
e disponível só via dashboard PowerBI — exigiria pipeline separada.

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.snis --no-fetch
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import time
import zipfile
from collections.abc import Iterator

import dlt
import xlrd

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_triples
from lakebrasil.common.s3 import RAW_BUCKET, list_keys, s3_client
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "snis/raw/"
ZIP_RE = re.compile(r"^Planilhas_AE(\d{4})\.zip$")

# (col_idx_in_xls, indicador_id_suffix, unidade) — based on header
# inspection of Planilha_LPU_Indicadores.xls (row 7 = main header).
INDICADORES = [
    (42, "IN055", "percentual"),  # atendimento total água
    (43, "IN023", "percentual"),  # atendimento urbano água
    (61, "IN049", "percentual"),  # perdas distribuição
    (64, "IN056", "percentual"),  # atendimento total esgoto
    (67, "IN015", "percentual"),  # coleta esgoto
    (68, "IN016", "percentual"),  # tratamento esgoto
]

# Linha do header principal no LPU_Indicadores.xls. Empíricamente
# row=7 contém os nomes; row=8 sub-header (unidade); data começa em
# row=9 mas as primeiras linhas podem ter '-' como placeholder
# (header secundário) — vamos validar via cell_value(0).
HEADER_ROW = 7
DATA_START_ROW = 9


def _build_co6_to_ibge() -> dict[int, int]:
    """6-dígito SNIS (IBGE truncado) → ibge_code 7-dig."""
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code",)
    ).to_arrow()
    out: dict[int, int] = {}
    for ibge in arrow.column("ibge_code").to_pylist():
        if ibge is None:
            continue
        out[int(ibge) // 10] = int(ibge)
    return out


def _build_co6_to_uf() -> dict[int, str]:
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf")
    ).to_arrow()
    out: dict[int, str] = {}
    for ibge, uf in zip(arrow.column("ibge_code").to_pylist(),
                        arrow.column("uf").to_pylist()):
        if ibge is None:
            continue
        out[int(ibge) // 10] = uf
    return out


def _iter_xls_bundles(zip_bytes: bytes, ano: int) -> Iterator[tuple[str, bytes]]:
    """Yield (label, xls_bytes) para todos os *Indicadores*.xls em LPU+LPR+LEP+Regionais.

    SNIS divide as planilhas por tipo de prestador. LPU tem ~1,860 munis
    (autarquias municipais), Regionais tem ~4,050 munis distribuídos em
    ~28 CESBs estaduais (Sabesp, Copasa, etc.) — somando os 4 bundles
    cobrimos ~80% dos 5,571 municípios.

    Estrutura do bundle varia por ano:
      2020: arquivos no root (Planilhas_AE2020_Completa_LPU.zip)
      2021: arquivos no root (Planilha_AE2021_Completa_LPU.zip)
      2022: aninhados em Planilhas_AE2022/
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf_outer:
        # 3 bundles single-file (LPU, LPR, LEP) — 1 .xls de indicadores cada
        for tipo in ("LPU", "LPR", "LEP"):
            inner = [n for n in zf_outer.namelist()
                     if n.endswith(f"_Completa_{tipo}.zip")]
            if not inner:
                continue
            sub_bytes = zf_outer.read(inner[0])
            with zipfile.ZipFile(io.BytesIO(sub_bytes)) as zf_inner:
                for n in zf_inner.namelist():
                    if "Indicadores" in n and n.endswith(".xls"):
                        yield (tipo, zf_inner.read(n))

        # Bundle Regionais — múltiplos CESBs, 1 pasta por operador
        reg = [n for n in zf_outer.namelist()
               if n.endswith("_Completa_Regionais.zip")]
        if reg:
            sub_bytes = zf_outer.read(reg[0])
            with zipfile.ZipFile(io.BytesIO(sub_bytes)) as zf_reg:
                for n in zf_reg.namelist():
                    if "Indicadores" in n and n.endswith(".xls"):
                        # Folder name = sigla do CESB (ex: SABESP)
                        cesb = n.split("/")[1] if "/" in n else "CESB"
                        yield (f"REG/{cesb}", zf_reg.read(n))


def _iter_snis_year(zip_bytes: bytes, ano: int,
                    co6_to_ibge: dict[int, int],
                    co6_to_uf: dict[int, str]) -> Iterator[dict]:
    """Emite indicadores combinando LPU + LPR + LEP + 28 Regionais (CESBs).

    Dedupe (ibge_code, indicador_id) — LPU vence (primeiro processado),
    Regionais preenche o resto. Em teoria mutuamente exclusivo (cada
    município tem 1 operador primário), mas defensivamente fazemos
    seen-set.
    """
    periodo = str(ano)
    fonte_arq = f"Planilhas_AE{ano}.zip"
    seen: set[tuple[int, str]] = set()  # (ibge, indicador) já emitido
    stats: dict[str, int] = {}  # label → emitted

    for label, xls_bytes in _iter_xls_bundles(zip_bytes, ano):
        wb = xlrd.open_workbook(file_contents=xls_bytes)
        sh = wb.sheet_by_index(0)
        emitted = 0
        for r in range(DATA_START_ROW, sh.nrows):
            cod_raw = sh.cell_value(r, 0)
            if not cod_raw or not str(cod_raw).replace('.', '').replace(',', '').isdigit():
                continue
            try:
                co6 = int(float(cod_raw))
            except (ValueError, TypeError):
                continue
            ibge = co6_to_ibge.get(co6)
            if ibge is None:
                continue
            uf = co6_to_uf.get(co6, "??")
            for col_idx, in_code, unidade in INDICADORES:
                key = (ibge, in_code)
                if key in seen:
                    continue
                v = sh.cell_value(r, col_idx)
                if v in ("", None, "-"):
                    continue
                try:
                    valor = float(v)
                except (ValueError, TypeError):
                    continue
                seen.add(key)
                yield {
                    "ibge_code":     ibge,
                    "uf":            uf,
                    "fonte":         "snis",
                    "indicador_id":  f"snis.{in_code}",
                    "periodo":       periodo,
                    "valor":         valor,
                    "valor_texto":   None,
                    "unidade":       unidade,
                    "fonte_arquivo": fonte_arq,
                }
                emitted += 1
        stats[label] = emitted

    summary = ", ".join(f"{k}={v}" for k, v in stats.items() if v > 0)
    total = sum(stats.values())
    print(f"  SNIS {ano}: {summary} → total {total:,}", file=sys.stderr)


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ano", type=int, action="append",
                   help="Anos a carregar (default: todos no raw).")
    add_common_args(p, table_default="indicadores_serie")
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        try:
            ensure_fetched("snis_*", refresh=args.refresh)
        except ValueError:
            print("  [warn] assumindo s3://data-br-raw/snis/raw/ já populado.",
                  file=sys.stderr)

    co6_to_ibge = _build_co6_to_ibge()
    co6_to_uf = _build_co6_to_uf()
    print(f"SNIS→IBGE índice: {len(co6_to_ibge):,} municípios", file=sys.stderr)

    already = set() if args.full_refresh else loaded_triples(
        "indicadores_serie", "fonte", "indicador_id", "periodo")

    @dlt.resource(
        name="indicadores_serie",
        primary_key=["ibge_code", "indicador_id", "periodo"],
        write_disposition="append",
    )
    def snis_indicadores() -> Iterator[dict]:
        s3 = s3_client()
        keys = sorted(list_keys(S3_PREFIX))
        anos_filter = set(args.ano) if args.ano else None
        for key in keys:
            name = key.rsplit("/", 1)[-1]
            m = ZIP_RE.match(name)
            if not m:
                continue
            ano = int(m.group(1))
            if anos_filter and ano not in anos_filter:
                continue
            t0 = time.monotonic()
            print(f"  SNIS download {name}", file=sys.stderr)
            body = s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
            for rec in _iter_snis_year(body, ano, co6_to_ibge, co6_to_uf):
                if ("snis", rec["indicador_id"], rec["periodo"]) in already:
                    continue
                yield rec
            print(f"  SNIS {ano}: pronto em {time.monotonic()-t0:.1f}s",
                  file=sys.stderr)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="snis_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(snis_indicadores())
        with pipe.sql_client() as c:
            print(c.execute_sql(
                "SELECT indicador_id, count(*), avg(valor) AS media_pct "
                "FROM indicadores_serie GROUP BY indicador_id "
                "ORDER BY count(*) DESC"))
        return 0

    pipe = dlt.pipeline(pipeline_name="snis", destination=s3tables_iceberg)
    info = pipe.run(snis_indicadores())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
