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

### Self-hosted (Iceberg full)

Pra escrever no Iceberg precisa de:
- AWS account com S3 Tables habilitado
- `ICEBERG_WAREHOUSE` env apontando pro ARN do table bucket
- Credentials (IAM role ou keys via `AWS_PROFILE`)

```bash
export AWS_PROFILE=meu-perfil
export ICEBERG_WAREHOUSE=arn:aws:s3tables:us-east-1:<seu-account>:bucket/<seu-bucket>
lakebrasil run rais --no-fetch
```

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
        │  _scripts/fetchers/             │  fetch → S3 raw
        │  (govbr F5 bypass, FTP, SIDRA   │  (idempotent, sha256-tracked)
        │   paginators, WebDAV auth)      │
        └─────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  _pipelines/<source>.py         │  stream parse + agg
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

`pipelines/_dq/` — declarative checks framework. Cada pipeline pode
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
- [ ] Rust REST API + MCP server sobre o lake

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
