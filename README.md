<p align="center">
  <img src="./assets/logo/wordmark.png" alt="lakebrasil" width="480"/>
</p>

# lakebrasil / data-br

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Rows](https://img.shields.io/badge/rows-44M+-green)](#whats-in-the-lake)
[![PyPI](https://img.shields.io/pypi/v/lakebrasil?label=pypi%3Alakebrasil)](https://pypi.org/project/lakebrasil/)

**Open source data engine for Brazilian public data.** 35 pipelines
extract, normalize, and aggregate data from 30+ government sources
(IBGE, MTE, MEC, INEP, INPE, ANS, BACEN, TSE, Câmara, Portal da
Transparência, ...) into an S3 + Iceberg lakehouse keyed by município.

Use it to build dashboards, journalism, academic research, civic tech,
or your own SaaS. No vendor lock-in — your AWS account, your data.

## What's in the lake

**17 tables, ~44M rows aggregated across 20 sources**, all keyed by
IBGE 7-digit `ibge_code`:

| Domain | Examples |
|---|---|
| **Demographics** | Censo IBGE, RM, CEPs, registro civil |
| **Economy** | RAIS (20 anos), CEMPRE, comex municipal, frota |
| **Public finance** | STN, FNDE Fundeb/SIOPE, orçamento, emendas |
| **Social** | BPC, Bolsa Família, Pé-de-Meia |
| **Education** | INEP Censo Escolar (5 anos), MEC ed superior |
| **Health** | DataSUS CNES, ANS planos saúde, SNIS/SINISA |
| **Agriculture** | IBGE PAM lavouras, PPM pecuária |
| **Environment** | INPE PRODES desmatamento Amazônia |
| **Politics** | TSE eleições, Câmara proposições/votações |
| **Public safety** | SINESP/MJSP indicators |
| **Companies** | CNPJ Receita Federal, ANEEL geração, ANP combustíveis |
| **Macroeconomics** | BACEN SGS (IPCA, Selic, câmbio), PNAD Contínua |

Schema central: `data_br.indicadores_serie` (~15M rows) — tabela longa
com `(ibge_code, fonte, indicador_id, periodo, valor)` que normaliza
20 fontes diferentes em um esquema único pra time-series queries.

## Quick start

### Install via pip

```bash
# core (35 pipelines HTTP/SIDRA básicas)
pip install lakebrasil

# com extras opcionais por tipo de fonte:
pip install 'lakebrasil[excel]'    # sinisa, mec_ed_superior (.xlsx)
pip install 'lakebrasil[archive]'  # rais_estab (.7z)
pip install 'lakebrasil[geo]'      # inpe (shapefile/geopandas)
pip install 'lakebrasil[all]'      # tudo
```

### CLI

```bash
lakebrasil list                          # lista 35 pipelines
lakebrasil run anp_precos --dry-run      # dry-run vai pra DuckDB local
lakebrasil run snis --no-fetch           # full → Iceberg
lakebrasil dq                            # data quality checks
lakebrasil fetch bacen_ipca_mensal       # baixa raw do gov.br
```

### Library

```python
from lakebrasil.pipelines import bpc, rais, sinisa
bpc.main()  # cada pipeline tem CLI próprio + entry point main()
```

### Self-host: backend pluggable (AWS S3 Tables OR 100% OSS)

O catalog é auto-detectado pela env `ICEBERG_WAREHOUSE`. Escolha um:

#### a) AWS S3 Tables (default — managed)
```bash
export ICEBERG_WAREHOUSE=arn:aws:s3tables:us-east-1:<account>:bucket/<name>
export AWS_PROFILE=meu-perfil   # ou IAM role no Fargate
lakebrasil run rais --no-fetch
```

#### b) 100% OSS local — MinIO + Nessie via Docker (zero AWS)
```bash
docker compose up -d            # MinIO (S3 :9100, console :9101) + Nessie (:19120)

export ICEBERG_WAREHOUSE=warehouse
export ICEBERG_REST_ENDPOINT=http://localhost:19120/iceberg/
export S3_ENDPOINT_URL=http://localhost:9100
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export DATA_BR_RAW_BUCKET=data-br-raw   # raw layer bucket (pre-criado pelo compose)

lakebrasil run bacen --refresh      # fetch BACEN SGS → MinIO → Iceberg → Nessie
```
Tabelas Iceberg são **auto-criadas** (a partir do schema do primeiro batch)
quando não existem. Set `DATA_BR_AUTOCREATE=0` pra exigir DDL prévio
(útil em prod com schemas curados via CDK).

Console MinIO: `http://localhost:9101` (minioadmin/minioadmin) · Nessie: `http://localhost:19120`

#### c) Vanilla REST catalog (Lakekeeper, Tabular, qualquer Iceberg REST)
```bash
export ICEBERG_WAREHOUSE=s3://meu-bucket/warehouse/
export ICEBERG_REST_ENDPOINT=https://meu-catalog.example.com/iceberg
export ICEBERG_REST_TOKEN=<bearer-token>   # opcional
lakebrasil run rais --no-fetch
```

#### d) Zero-infra dev — SQLite + filesystem local
```bash
pip install 'lakebrasil[local]'   # pulls in pyiceberg's sql-sqlite extra (sqlalchemy)

export ICEBERG_WAREHOUSE=local:///tmp/lakebrasil-warehouse
lakebrasil run anp_precos --no-fetch   # real Iceberg write — SQLite catalog + local Parquet files
```
No AWS account, no Docker — real Iceberg tables (queryable with DuckDB) on local disk.

Cada pipeline tem docstring com fonte/cobertura/caveats —
veja [`pipelines/lakebrasil/pipelines/`](./pipelines/lakebrasil/pipelines/).

## Pipelines disponíveis (35)

```
aneel.py            anp_postos.py       anp_precos.py       ans.py
bacen.py            beneficios.py       bolsa_familia.py    bpc.py
camara.py           ceps.py             cnpj.py             cnpj_empresas.py
comex.py            datasus_cnes.py     emendas.py          fnde.py
frota.py            ibge_geo.py         inep.py             inpe.py
mec_ed_superior.py  municipios.py       orcamento.py        pam.py
pe_de_meia.py       pnadc.py            ppm.py              rais.py
rais_estab.py       sancoes.py          sinesp.py           sinisa.py
snis.py             stn.py              tse.py
```

Cada uma com sub-200 linhas de Python, sem framework pesado, fácil de
forkar/modificar.

## Architecture

```
        Government source (HTTP/FTP/WebDAV/SIDRA API)
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  lakebrasil/fetchers/           │  fetch → S3 raw
        │  (govbr F5 bypass, FTP, SIDRA   │  (idempotent, sha256-tracked)
        │   paginators, WebDAV auth)      │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  lakebrasil/pipelines/<src>.py  │  stream parse + agg
        │  (defaultdict per município,    │  por município/período
        │   ThreadPool, no pandas)        │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  dlt + pyiceberg                │  write → S3 Iceberg
        │  (append-only, snapshot iso,    │  via REST catalog
        │   resume via loaded_triples)    │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  Athena / DuckDB / Spark        │  query
        │  (Glue federation pra Athena)   │
        └─────────────────────────────────┘
```

## Data quality

`pipelines/lakebrasil/dq/` — declarative checks framework. Cada pipeline pode
declarar:
- Row count thresholds (min/max esperado por período)
- Schema validation (colunas requeridas, tipos)
- Range checks (valores numéricos plausíveis)
- Cross-table consistency (ex: pop_urb ≤ pop_total)

Failures bloqueiam o load step.

## Why open source?

Os dados são o moat, não o código. Dados públicos brasileiros estão
fragmentados em 30+ órgãos federais, cada um com seus quirks (encoding,
códigos truncados, ZIP-em-ZIP, FTP only, WFS quebrado). O trabalho está
nos 40+ edge cases documentados nas pipelines. Open-sourcing o engine
significa:

1. Qualquer pessoa pode verificar como indicadores são calculados
   (auditoria LGPD, reprodutibilidade acadêmica, jornalismo de dados)
2. Contribuintes consertam bugs de encoding mais rápido que mantenedores
   sozinhos
3. Self-hosters que não querem vendor lock-in têm um caminho limpo
4. Pesquisadores universitários, ONGs, jornalistas e cívic-techs ganham
   uma base comum sobre a qual construir

## Roadmap

- [ ] CAGED novo (saldo emprego mensal)
- [ ] DataSUS SIM + SINASC (mortalidade/nascimentos)
- [ ] INEP ENEM/SAEB microdados
- [ ] PNCP compras públicas
- [ ] TransfereGov convênios federais
- [ ] Querido Diário (diários oficiais municipais)

**MCP server:** já existe, em repo separado — [lakebrasil-mcp](https://github.com/lakebrasil/mcp),
86 tools sobre dados ao vivo (não sobre o lake — consulta a fonte original em tempo real).

Issues abertas: [github.com/lakebrasil/data-br/issues](https://github.com/lakebrasil/data-br/issues).

## Contributing

Bem-vindo. Veja [CONTRIBUTING.md](./CONTRIBUTING.md) pra escopo, padrões
de código, e template de PR.

Bug em dado (ex: município X com valor errado)? Abra issue com label
`data-quality` mencionando `fonte`, `indicador_id`, `periodo`, e a
fonte oficial cruzada.

## License

MIT — see [LICENSE](./LICENSE).

Cite como:
> data-br — Brazilian Public Data Engine. https://github.com/lakebrasil/data-br
