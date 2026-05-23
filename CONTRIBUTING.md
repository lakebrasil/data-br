# Contributing to data-br

Obrigada por considerar contribuir! Este projeto é **MIT open source** —
todo o engine de extração de dados (`pipelines/lakebrasil/`) é livre
pra qualquer um usar, self-hospedar, comercializar, ou forkar.

## O que dá pra contribuir

### 🟢 Bem-vindo
- **Novas pipelines** (`pipelines/lakebrasil/pipelines/*.py`) — qualquer dataset
  público brasileiro com granularidade município (ou que dê pra agregar)
- **Bug fixes** em pipelines existentes (encoding, separator, schema drift)
- **Melhorias de performance** (paralelização, streaming, push-down filters)
- **Schema docs** — completar docstring de pipelines com fonte/cobertura/caveats
- **Data quality checks** em `pipelines/_dq/`
- **Reprocessamentos** quando algum órgão republica histórico

### 🟡 Discuta antes (abra issue)
- Mudanças no schema central `data_br.indicadores_serie` (downstream breakage)
- Novas dependências Python pesadas (queremos manter o core leve;
  use `[extras]` se possível)
- Pipelines que demandam credenciais privadas
- Mudanças no `lakebrasil/loaders/iceberg.py` (catalog config)

### 🔴 Não aceito
- Web scraping de fontes que proíbem em ToS
- Datasets que violam LGPD (PII sem anonimização)
- Código que injeta telemetria/tracking sem opt-in explícito

## Quick start

```bash
# 1. Clone
git clone https://github.com/lakebrasil/data-br
cd data-br

# 2. Setup Python env
python3 -m venv .venv && source .venv/bin/activate
pip install -e pipelines/

# 3. Roda um pipeline local em dry-run (vai pra duckdb, não Iceberg)
cd pipelines
lakebrasil run anp_precos --dry-run --no-fetch
```

Pra escrever uma pipeline nova, copie o template mais próximo:
- **CSV mensal/anual por município (SIAFI)** → `bpc.py` ou `bolsa_familia.py`
- **SIDRA IBGE com agregação** → `pam.py` ou `ppm.py`
- **XLSX multi-sheet** → `sinisa.py` ou `mec_ed_superior.py`
- **Shapefile/GeoJSON** → `inpe.py`
- **API REST direta** → `bacen.py`

## Padrões de código

- **Sem secrets em commits** — use env vars (`AWS_*`, `ICEBERG_WAREHOUSE`, tokens)
- **Sem account IDs hardcoded** — `arn:aws:s3tables:...:bucket/...` vem de env
- **fonte` em snake_case** (`'rais'`, `'bpc'`, `'mec_es'`)
- **`indicador_id`** = `'<fonte>.<descritor>'` (ex: `'rais.vinculos_ativos'`)
- **`ibge_code` sempre 7-dígitos** — usa `co6_to_ibge` map se source vem com 6
- **Stream parse** pra CSVs > 100MB — evita pandas/RAM
- **Stderr `-u` unbuffered** pra logs visíveis em backgrounds
- **`loaded_triples(fonte=...)`** push-down filter — não scan a tabela inteira
- **Validação** em dry-run com `--dry-run` (DuckDB local) antes de Iceberg

## Pull Request flow

1. Fork + branch `feat/<source>` ou `fix/<bug-num>`
2. **Inclua docstring rica** no topo da pipeline:
   - Source URL oficial
   - Schema esperado das colunas usadas
   - Cobertura (anos, municípios)
   - Caveats conhecidos (ex: SIDRA flaky, separator quirks)
3. **Dry-run validado** localmente — comente no PR os números (rows, munis,
   indicadores únicos)
4. **Commit message** seguindo o padrão dos commits existentes:
   - `feat(<fonte>): descrição curta`
   - `fix(<fonte>): bug específico`
   - `docs(<fonte>): caveat / cobertura`
5. PR pra `main` — review focado em: correção schema, performance, secrets.

## Reportar bugs de dados

Se você notar valor errado/faltante (ex: município X com população 0):
1. Abra issue com label `data-quality`
2. Inclua: `fonte`, `indicador_id`, `periodo`, valor esperado vs observado
3. Cite a fonte oficial cruzada (publicação IBGE/MTE/etc)

## Código de Conduta

Seja gentil. Brazil dev community já é pequena demais. Discordâncias
técnicas são bem-vindas; ataques pessoais não são.

---

Licença: MIT (veja [LICENSE](./LICENSE)). Ao enviar PR você concorda em
licenciar sua contribuição sob os mesmos termos.
