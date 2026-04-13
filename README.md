# DATASUS · SIH/SUS — Sistema Completo

Extração, armazenamento e visualização de dados de Produção Hospitalar do DATASUS.

---

## Estrutura de Arquivos

```
projeto/
├── datasus_scraper.py       # 1. Robô Playwright — extrai CSVs do DATASUS
├── datasus_loader.py        # 2. Carrega CSVs no banco SQLite
├── app.py                   # 3. Dashboard Streamlit
├── generate_sample_data.py  # Gera dados de amostra (para testes sem scraper)
├── baixados/                # CSVs baixados pelo scraper
└── datasus.db               # Banco de dados SQLite
```

---

## Pré-requisitos

```bash
pip install playwright pandas streamlit plotly
playwright install chromium
```

---

## Passo a Passo

### Passo 1 — Scraper (extração dos dados)

```bash
python datasus_scraper.py
```

O scraper acessa automaticamente:
- **Site:** https://tabnet.datasus.gov.br/cgi/deftohtm.exe?sih/cnv/spabr.def
- **Linha:** Município
- **Coluna:** Subgrupo de Procedimentos
- **Conteúdo:** Qtd. Aprovada e Valor Aprovado (2 execuções)
- **Períodos:** Jan/2024 a Jan/2026 (25 meses)
- **Opções:** Exibir linhas zeradas + separador `;`

Arquivos gerados em `baixados/`:
```
baixados/
  datasus_municipio_subgrupo_Qtd_aprovada.csv
  datasus_municipio_subgrupo_Val_aprovado.csv
```

> ⏱️ Estimativa: ~30–60 min para os 25 meses × 2 conteúdos

---

### Passo 2 — Carregar no Banco

```bash
python datasus_loader.py
```

Cria `datasus.db` (SQLite) com a tabela:

```sql
producao_hospitalar (
    id                    INTEGER PRIMARY KEY,
    municipio             TEXT,
    subgrupo_procedimento TEXT,
    periodo               TEXT,      -- ex: "Jan/2024"
    mes                   INTEGER,
    ano                   INTEGER,
    tipo                  TEXT,      -- 'Qtd_aprovada' | 'Val_aprovado'
    valor                 REAL
)
```

---

### Passo 3 — Dashboard Streamlit

```bash
streamlit run app.py
```

Acesse: http://localhost:8501

#### Funcionalidades:

| Seção | Descrição |
|-------|-----------|
| **KPIs** | Total, média, nº municípios, nº registros |
| **1 · Lista** | Tabela filtrada com exportação CSV |
| **2 · Estatísticas** | Describe completo, por município e subgrupo |
| **3.1** | Evolução mensal (área) |
| **3.2** | Top N municípios (barras horizontais) |
| **3.3** | Composição por subgrupo (pizza + treemap) |
| **3.4** | Heatmap município × período |
| **3.5** | Linhas por subgrupo ao longo do tempo |
| **3.6** | Comparativo anual agrupado |
| **3.7** | Dispersão Qtd × Valor |

---

### Testes sem Scraper (dados de amostra)

```bash
python generate_sample_data.py
streamlit run app.py
```

---

## Notas Técnicas

- O scraper usa **Playwright** em modo headless (sem janela visível)
- O banco é **SQLite** — sem necessidade de servidor de banco de dados
- O dashboard usa **Streamlit + Plotly** para visualizações interativas
- Filtros na sidebar afetam todos os gráficos em tempo real
- Dados podem ser exportados como CSV diretamente do dashboard
