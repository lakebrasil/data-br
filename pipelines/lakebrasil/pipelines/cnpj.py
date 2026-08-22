"""CNPJ Receita Federal (Empresas + Estabelecimentos) → cnpj_estabelecimentos_municipio.

Dump mensal da Receita Federal em
`s3://data-br-raw/cnpj/raw/`:
  - Empresas{0..9}.zip
  - Estabelecimentos{0..9}.zip   ← pesados (~2 GB cada)
  - Socios{0..9}.zip
  - Cnaes.zip, Motivos.zip, Municipios.zip, Naturezas.zip, Paises.zip,
    Qualificacoes.zip, Simples.zip                    (lookup tables)

Saída: agregado por (snapshot, uf, municipio_codigo_rf, ibge_code,
cnae_principal, situacao_cadastral) → COUNT. Reduz ~55M linhas
estabelecimentos × 50 GB CSV pra ~1-2M linhas analíticas.

Resolução IBGE: o CNPJ usa o `código de município` da Receita (4
dígitos), distinto do IBGE 7-dígitos. Joinamos via `Municipios.zip`
(codigo_rf, nome) → nome slugificado → `data_br.municipios.slug` →
ibge_code.

Idempotência: lista snapshots já em Iceberg e pula. `--full-refresh`
reprocessa do zero (DROP TABLE antes pra evitar duplicação).

⚠ Catalog/fetcher pendentes (issue #13): a Receita publica via WebDAV
com auth básica — sem fetcher ainda. Por ora `--no-fetch` é o default
implícito (ensure_fetched é tentado mas não falha se sem catalog).

Uso:
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.cnpj --snapshot 2026-04 --dry-run
    AWS_PROFILE=<seu-perfil> python -m lakebrasil.pipelines.cnpj --snapshot 2026-04
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import unicodedata
import zipfile
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import dlt

from lakebrasil.common.args import add_common_args
from lakebrasil.common.fetch import ensure_fetched
from lakebrasil.common.incremental import loaded_snapshots
from lakebrasil.common.s3 import RAW_BUCKET, download_to_file, get_object_bytes, list_keys
from lakebrasil.pipelines.destinations.s3tables import s3tables_iceberg

S3_PREFIX = "cnpj/raw/"
ESTAB_FILE_RE  = re.compile(r"^Estabelecimentos(\d+)\.zip$")
MUNI_FILENAMES = ("Municipios.zip", "Municipios0.zip")

# Receita Federal layout, sem header. Indices 0-based.
# Estabelecimentos:
#   0=cnpj_basico  1=cnpj_ordem  2=cnpj_dv  3=mat_filial  4=nome_fantasia
#   5=situacao_cad  6=data_sit_cad  7=motivo  8=cidade_ext  9=pais
#   10=data_inicio  11=cnae_principal  12=cnae_secundaria
#   13-18=endereco  19=uf  20=municipio_rf  21+=...
ESTAB_COLS = {
    "uf":                 19,
    "municipio_rf":       20,
    "cnae_principal":     11,
    "situacao_cadastral":  5,
}

# Municipios.zip CSV: 0=codigo_rf, 1=descricao
MUNI_COL_CODIGO = 0
MUNI_COL_NOME   = 1


def _slug(text: str) -> str:
    """Normalização que bate com `data_br.municipios.slug` (Sao Paulo →
    sao-paulo). Idem ao `lakebrasil.common.enrich._slug` mas inline pra não
    forçar o load do _municipios_index aqui (queremos build próprio)."""
    if not text:
        return ""
    s = unicodedata.normalize("NFKD", text)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def _build_rf_to_ibge(snapshot: str) -> dict[str, int]:
    """codigo_rf (RFB municipality) → ibge_code (IBGE 7-dígitos).

    Receita Federal não publica crosswalk oficial, só `codigo_rf, nome`.
    Bate slug do nome contra `data_br.municipios.slug`. Match rate
    típico: ~98% (5570 / 5670 do RF — sobram códigos extintos / fora-BR).
    """
    # 1. Carrega municipios dim do Iceberg.
    from lakebrasil.loaders.iceberg import catalog
    arrow = catalog().load_table("data_br.municipios").scan(
        selected_fields=("ibge_code", "uf", "slug")
    ).to_arrow()
    slug_to_ibge: dict[tuple[str, str], int] = {}
    for ibge, uf, slug in zip(
        arrow.column("ibge_code").to_pylist(),
        arrow.column("uf").to_pylist(),
        arrow.column("slug").to_pylist(),
    ):
        if uf and slug:
            slug_to_ibge[(uf.upper(), slug)] = int(ibge)

    # 2. Baixa Municipios.zip do CNPJ snapshot (escolhe primeiro que existir).
    keys = set(list_keys(S3_PREFIX))
    muni_key = None
    for fname in MUNI_FILENAMES:
        candidate = f"{S3_PREFIX}{fname}"
        if candidate in keys:
            muni_key = candidate
            break
    if muni_key is None:
        raise RuntimeError(
            f"Municipios.zip não encontrado em s3://{RAW_BUCKET}/{S3_PREFIX} — "
            f"upload do dump CNPJ {snapshot} parece incompleto"
        )

    # Municipios.zip do CNPJ não traz UF — match é puramente por slug do
    # nome contra `data_br.municipios.slug`. Colisões entre UFs com slug
    # igual são raras (~10 casos); pega o primeiro match (suficiente
    # pra primeiro pipeline; refinamento futuro: cruzar com prefixo
    # numérico do código RF que costuma codificar UF).
    slug_only_to_ibge: dict[str, int] = {}
    for (_uf_key, slug_key), ibge in slug_to_ibge.items():
        slug_only_to_ibge.setdefault(slug_key, ibge)

    import csv
    import io
    tmp = Path("/tmp") / "cnpj_Municipios.zip"
    download_to_file(muni_key, tmp)
    rf_to_ibge: dict[str, int] = {}
    misses: list[str] = []
    try:
        with zipfile.ZipFile(tmp) as zf:
            inner = next(n for n in zf.namelist() if not n.endswith("/"))
            with zf.open(inner) as fh:
                text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="")
                reader = csv.reader(text_io, delimiter=";", quotechar='"')
                for row in reader:
                    if len(row) <= MUNI_COL_NOME:
                        continue
                    codigo = row[MUNI_COL_CODIGO].strip()
                    nome = row[MUNI_COL_NOME].strip()
                    if not codigo or not nome:
                        continue
                    ibge = slug_only_to_ibge.get(_slug(nome))
                    if ibge is not None:
                        rf_to_ibge[codigo] = ibge
                    elif len(misses) < 10:
                        misses.append(f"{codigo}/{nome}")
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass

    print(f"CNPJ→IBGE índice: {len(rf_to_ibge)} matched, "
          f"{len(misses)} missing (samples: {misses})", file=sys.stderr)
    return rf_to_ibge


def _aggregate_estab_zip_into(
    s3_key: str,
    counter: Counter[tuple[str, str, str, str]],
) -> int:
    """Streama um Estabelecimentos{N}.zip e SOMA seus counts em `counter`
    (mutado in-place). Retorna o # de rows lidas no zip.

    Por que Counter compartilhado em vez de per-zip + merge: a mesma
    chave (uf, municipio_rf, cnae, situacao) aparece em vários zips
    porque a RFB particiona Estabelecimentos por hash do CNPJ, não por
    geografia/setor. Per-zip yield gerava ~3× duplicatas (66% dup rate
    medido em mai/2026); accumulator único elimina by construction.

    Por que Python e não DuckDB? Estabelecimentos vem latin-1 com bytes
    cp1252 (0x80-0x9F) que o leitor latin-1 do DuckDB rejeita ("File is
    not latin-1 encoded"). Streaming Python com `csv` é mais lento mas
    blindado a encoding. Counter agregado em memória — ~5M chaves
    distintas no total (todos zips), ~400 MB pico. Fargate task tem
    16 GB, comporta.
    """
    name = s3_key.rsplit("/", 1)[-1]
    print(f"  CNPJ {name}: download S3 → memória", file=sys.stderr)
    body = get_object_bytes(s3_key)

    rows_seen = 0
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        for inner in zf.namelist():
            if inner.endswith("/"):
                continue
            with zf.open(inner) as fh:
                # latin-1 sempre decodifica (0-255 todos válidos);
                # cp1252 daria nome melhor pros bytes 0x80-0x9F mas a
                # diferença é estética e os codes RF/CNAE são ASCII.
                text_io = io.TextIOWrapper(fh, encoding="latin-1", newline="")
                reader = csv.reader(text_io, delimiter=";", quotechar='"')
                for row in reader:
                    if len(row) <= ESTAB_COLS["municipio_rf"]:
                        continue
                    uf = (row[ESTAB_COLS["uf"]] or "").strip()
                    if not uf:
                        continue
                    counter[(
                        uf,
                        (row[ESTAB_COLS["municipio_rf"]] or "").strip(),
                        (row[ESTAB_COLS["cnae_principal"]] or "").strip(),
                        (row[ESTAB_COLS["situacao_cadastral"]] or "").strip(),
                    )] += 1
                    rows_seen += 1
                    if rows_seen % 1_000_000 == 0:
                        print(f"  CNPJ {name}: {rows_seen:,} rows lidas "
                              f"({len(counter):,} chaves totais acumuladas)",
                              file=sys.stderr)

    print(f"  CNPJ {name}: total rows={rows_seen:,} (acumulador "
          f"agora com {len(counter):,} chaves)", file=sys.stderr)
    return rows_seen


def _emit_aggregated(
    counter: Counter[tuple[str, str, str, str]],
    snapshot: str,
    rf_to_ibge: dict[str, int],
) -> Iterator[dict]:
    """Materializa o counter agregado (post-merge entre TODOS os zips)
    em rows pra dlt. Garante 0 dups por (snapshot,uf,municipio_rf,cnae,
    situacao) — compromisso com a PK declarada em CNPJ_ESTAB_SCHEMA."""
    for (uf, municipio_rf, cnae, situacao), qtd in counter.items():
        yield {
            "snapshot":            snapshot,
            "uf":                  uf,
            "municipio_codigo_rf": municipio_rf or None,
            "ibge_code":           rf_to_ibge.get(municipio_rf) if municipio_rf else None,
            "cnae_principal":      cnae or None,
            "situacao_cadastral":  situacao or None,
            "qtd":                 qtd,
        }


def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--snapshot", required=True,
                   help="YYYY-MM do dump RF — vai pra coluna snapshot.")
    p.add_argument("--limit", type=int,
                   help="Processa só os primeiros N zips de Estabelecimentos.")
    add_common_args(p, include_table=False)
    return p.parse_args()


def main() -> int:
    args = _build_args()

    if not args.no_fetch:
        # CNPJ ainda não tem entrada em catalog.yaml (precisa fetcher
        # webdav). Tenta best-effort; ignora ValueError("no catalog
        # sources match").
        try:
            ensure_fetched("cnpj_*", refresh=args.refresh)
        except ValueError as e:
            print(f"  [warn] {e}", file=sys.stderr)
            print("  [warn] assumindo s3://data-br-raw/cnpj/raw/ "
                  "já populado externamente.", file=sys.stderr)

    snapshot = args.snapshot
    already = set() if args.full_refresh else loaded_snapshots(
        "cnpj_estabelecimentos_municipio"
    )
    if snapshot in already:
        print(f"snapshot {snapshot} já em Iceberg — usar --full-refresh "
              f"pra reprocessar (drop table antes).", file=sys.stderr)
        return 0

    rf_to_ibge = _build_rf_to_ibge(snapshot)

    @dlt.resource(name="cnpj_estabelecimentos_municipio",
                  write_disposition="append")
    def cnpj_estab() -> Iterator[dict]:
        keys = sorted(list_keys(S3_PREFIX))
        estab_keys = [k for k in keys
                      if ESTAB_FILE_RE.match(k.rsplit("/", 1)[-1])]
        if args.limit:
            estab_keys = estab_keys[: args.limit]
        # Counter compartilhado: somamos qtd entre TODOS os zips antes
        # de emitir. Evita dups na PK (snapshot,uf,municipio,cnae,sit).
        accumulator: Counter[tuple[str, str, str, str]] = Counter()
        for key in estab_keys:
            t0 = time.monotonic()
            n = _aggregate_estab_zip_into(key, accumulator)
            print(f"  CNPJ {key.rsplit('/',1)[-1]}: {n:,} rows lidas em "
                  f"{time.monotonic()-t0:.1f}s", file=sys.stderr)
        print(f"  CNPJ: emitindo {len(accumulator):,} chaves agregadas",
              file=sys.stderr)
        yield from _emit_aggregated(accumulator, snapshot, rf_to_ibge)

    if args.dry_run:
        pipe = dlt.pipeline(pipeline_name="cnpj_dryrun", destination="duckdb",
                            dataset_name="staging", dev_mode=True)
        pipe.run(cnpj_estab())
        with pipe.sql_client() as c:
            print(c.execute_sql("SELECT count(*), count(ibge_code), sum(qtd) "
                                "FROM cnpj_estabelecimentos_municipio"))
            print(c.execute_sql("SELECT uf, sum(qtd) AS total FROM "
                                "cnpj_estabelecimentos_municipio "
                                "GROUP BY uf ORDER BY total DESC LIMIT 10"))
        return 0

    pipe = dlt.pipeline(pipeline_name="cnpj", destination=s3tables_iceberg)
    info = pipe.run(cnpj_estab())
    print(info)
    return 0


if __name__ == "__main__":
    sys.exit(main())
