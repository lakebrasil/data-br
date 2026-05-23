# data-br/pipelines

Cloud-native ELT do datalake `data-br`: catálogo declarativo →
fetcher streaming pra S3 → pipeline dlt agrega/enrichece/carrega
em S3 Tables Iceberg.

## Layout

```
pipelines/
├── _scripts/
│   ├── catalog.yaml          # 17 fontes catalogadas (extensível)
│   ├── catalog.py            # parser + expansão de generators
│   ├── fetch.py              # CLI: python -m _scripts.fetch
│   └── fetchers/             # http, bcb_sgs, transparencia, govbr
├── _pipelines/
│   ├── _fetch.py             # bridge: pipelines chamam ensure_fetched()
│   ├── _s3.py                # boto3 + DuckDB s3:// helpers
│   ├── _enrich.py            # municipios dim + resolve_ibge(uf, nome)
│   ├── destinations/
│   │   └── s3tables.py       # custom dlt destination → pyiceberg append
│   ├── bacen.py              # BCB SGS séries → data_br.macro_serie
│   ├── sancoes.py            # CEIS+CNEP → data_br.sancoes
│   ├── anp_postos.py         # postos varejistas → data_br.anp_postos
│   ├── emendas.py            # emendas → data_br.emendas_parlamentares
│   ├── orcamento.py          # despesas → data_br.orcamento_execucao_municipal
│   └── beneficios.py         # BPC + BF → data_br.beneficios_municipais
└── _loaders/
    └── iceberg.py            # REST catalog SigV4 (S3 Tables)
```

Schemas Iceberg vivem em `infra-cdk/lib/workloads/data-br/stacks/s3tables-stack.ts`
(CDK = single source of truth). Pipelines não criam tabela — assumem
que o stack já provisionou.

`{source}/raw/` dirs **não existem mais em disco** — todo raw vive em
`s3://data-br-raw/{source}/raw/{file}`.

## Storage layout S3

```
s3://data-br-raw/
├── {source}/raw/{file}                  # raw artefatos (CSV, JSON, zip)
└── _manifest/{source}/{stem}.manifest.json   # url, sha256, bytes, fetched_at

s3://data-br-tables/                     # S3 Tables (managed Iceberg)
└── data_br.{table}                      # municipios, ceps, macro_serie,
                                         # comex_municipio, frota_municipio,
                                         # eleicoes_municipais, indicadores_serie,
                                         # empresas_municipio_cnae, sancoes,
                                         # anp_postos, emendas_parlamentares,
                                         # orcamento_execucao_municipal,
                                         # beneficios_municipais
```

## Ciclo end-to-end

Cada pipeline é uma chamada:

```bash
AWS_PROFILE=<seu-perfil> python -m _pipelines.<source>
```

O que acontece:

1. **fetch (catalog-driven)** — `ensure_fetched("<source-pattern>")` lê
   `catalog.yaml`, dispatches pro fetcher certo (http / bcb_sgs /
   transparencia / govbr), faz streaming URL → S3 multipart upload,
   escreve manifest com sha256. Idempotente: pula se manifest match.
2. **dlt extract** — `@dlt.resource` itera `s3:// list_keys()`, baixa /
   stream cada arquivo, normaliza tipos.
3. **enrichment** — pipelines com `(uf, municipio)` string resolvem
   `ibge_code` via `resolve_ibge()` (slug match no dim municipios).
4. **dlt load → custom destination** — recebe `pa.RecordBatch`, casta
   pro schema Iceberg, reorder colunas, `pyiceberg.table.append()`
   atômico via REST catalog SigV4 do S3 Tables.

Flags em todo pipeline:
- `--no-fetch` — pula etapa 1 (raw já em S3 ou era pra estar)
- `--refresh` — força re-download mesmo com manifest fresh
- `--dry-run` — stage só em DuckDB local pra inspeção
- `--table <name>` — sobrescreve tabela alvo (POC: use `<name>_dlt_test`)

## Status atual das tabelas

| Tabela Iceberg | Origem | Linhas | ibge_code? |
|---|---|---|---|
| municipios | municipios-br seed | 5.571 | chave |
| ceps | municipios-br seed | 1.277.567 | via ibge_code |
| macro_serie | bacen | 11.057 | n/a (séries macro) |
| comex_municipio | comex | 16.327.359 | direto |
| frota_municipio | denatran | 132.374 | direto |
| eleicoes_municipais | tse | 2.097.961 | direto |
| indicadores_serie | snis + ana | 172.839 | direto |
| empresas_municipio_cnae | (cnpj — pendente) | 0 | direto |
| sancoes | ceis + cnep | 24.139 | via cpf_cnpj (chain dep cnpj) |
| anp_postos | anp | 46.035 | enrichment 99,8% |
| emendas_parlamentares | emendas | 91.900 | direto 40,7% |
| orcamento_execucao_municipal | orcamento | (pendente) | enrichment |
| beneficios_municipais | bpc + bf | (em load) | via SIAFI→IBGE map |

## Pendente

- **cnpj → empresas_municipio_cnae** — schema existe; pipeline ainda
  precisa lidar com ~7 GB de zips (Empresas + Estabelecimentos + Cnaes
  joined).
- **camara_proposicoes/votacoes/votos** — JSON aninhado, schema novo.
- **datasus CNES → estabelecimentos_saude** — schema novo.
- **inep censo escolar → matriculas_municipio** — schema novo.
- **18 fontes não-catalogadas** (mec, ans, datasus, inpe, etc.) —
  raw fetched ad-hoc, esperando entrarem em `catalog.yaml` + pipeline.

## Custos

S3 raw bucket (us-east-1, standard tier): ~50 GB × $0.023/GB-mo = **~$1.15/mo**.
S3 Tables bucket: storage + per-request, sub-$1/mo no volume atual.
Pipelines rodam local (dev) — sem custo de compute.
