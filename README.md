# Extracao Dados Financeiros

[![CI - Verificação de Código](https://github.com/Rodrigo-Henrique21/techCare_TI_money/actions/workflows/ci-verificacao-codigo.yml/badge.svg?branch=main)](https://github.com/Rodrigo-Henrique21/techCare_TI_money/actions/workflows/ci-verificacao-codigo.yml)
[![CI - Testes e Cobertura](https://github.com/Rodrigo-Henrique21/techCare_TI_money/actions/workflows/code-coverage.yml/badge.svg?branch=main)](https://github.com/Rodrigo-Henrique21/techCare_TI_money/actions/workflows/code-coverage.yml)
[![CI - Pre-commit](https://github.com/Rodrigo-Henrique21/techCare_TI_money/actions/workflows/pre-commit-hooks.yml/badge.svg?branch=main)](https://github.com/Rodrigo-Henrique21/techCare_TI_money/actions/workflows/pre-commit-hooks.yml)

Projeto para extração de dados públicos financeiros (B3, Tesouro Direto, BACEN, CVM, IBGE).
Projeto para extração de dados públicos financeiros (B3, BACEN e índices globais via brapi.dev).

## Delta Live Tables (Databricks)

O pipeline Delta Live Tables foi organizado de forma modular para facilitar a publicação no workspace do Databricks:

- **`pipelines/pipeline_financeiro.py`** – ponto de entrada do pipeline. Ele garante que o pacote esteja no `sys.path` e importa as camadas Bronze, Prata e Ouro.
- **`transformacoes/`** – diretório com módulos separados por camada (`bronze.py`, `prata.py`, `ouro.py`).
- **`utilitarios/`** – funções compartilhadas para configuração do catálogo/esquemas, captura das APIs externas e criação de estruturas auxiliares.

> 📁 No workspace do Databricks mantenha exatamente essa hierarquia (`pipelines/`, `transformacoes/`, `utilitarios/`). Os módulos deixam de depender de fallbacks dinâmicos e passam a exigir os caminhos corretos para evitar ambiguidades.

As camadas tratam integrações da B3, do BACEN e de **índices globais** (derivativos de índices) via `brapi.dev`, replicando o fluxo original dos scripts Python e estendendo para o universo de índices.

### Tabelas por camada

| Camada | Tabelas geradas | Descrição resumida |
|--------|------------------|--------------------|
| **Bronze** | - `platfunc.aafn_ing.cotacoes_b3`  <br> - `platfunc.aafn_ing.series_bacen`  <br> - `platfunc.aafn_ing.indices_futuros` | - Captura dados **brutos** de cotações da B3 (Yahoo Finance). <br> - Captura dados **brutos** das séries temporais do BACEN (SGS). <br> - Captura dados **brutos e históricos** de índices globais via `brapi.dev`, expandindo `historicalDataPrice` em 1 linha por índice/dia, com metadados (região, categoria, 52w high/low) e `ingestion_timestamp`. |
| **Prata** | - `platfunc.aafn_tgt.cotacoes_b3`  <br> - `platfunc.aafn_tgt.series_bacen`  <br> - `platfunc.aafn_tgt.indices_futuros` | - Padroniza esquemas e tipos de dados. <br> - Aplica validações de qualidade com `dlt.expect`. <br> - Remove inconsistências e registros inválidos. <br> - Normaliza o histórico diário dos índices globais, garantindo tipos consistentes (preços, volume, datas) e enriquecendo com taxonomia (`indice`, `regiao`, `categoria`). |
| **Ouro** | - `platfunc.aafn_ddm.metricas_b3`  <br> - `platfunc.aafn_ddm.indicadores_bacen`  <br> - `platfunc.aafn_ddm.indices_futuros_metricas` | - Consolida KPIs das ações acompanhadas (B3). <br> - Gera um resumo consolidado das principais séries do BACEN. <br> - Consolida métricas de desempenho dos índices globais (retornos, volatilidade, distância para 52w high/low, médias móveis etc.), prontas para consumo analítico e dashboards. |
### Configuração de índices globais

Os índices globais são configurados via um dicionário padrão em `utilitarios/configuracoes.py` (`INDICES_FUTUROS_PADRAO`), contendo:

- `ticker` (símbolo usado na `brapi.dev`, ex.: `^GSPC`, `^IXIC`, `^DJI`, `IFIX.SA`)
- `regiao` (ex.: `NA`, `EU`, `APAC`, `LATAM`)
- `categoria` (ex.: `Ações`, `Imobiliário`, etc.)

Essa configuração pode ser sobrescrita via `spark.conf` como JSON usando a chave:

- `techcare.indices.futuros.config`

Exemplo de JSON mínimo:

```json
{
  "IndSp500":   {"ticker": "^GSPC",    "regiao": "NA",   "categoria": "Ações"},
  "IndNasdaq":  {"ticker": "^IXIC",    "regiao": "NA",   "categoria": "Ações"},
  "IndBovespa": {"ticker": "^BVSP",    "regiao": "LATAM","categoria": "Ações"},
  "IndIFIX":    {"ticker": "IFIX.SA",  "regiao": "LATAM","categoria": "Imobiliário"}
}
```

### Como configurar o pipeline

1. No Databricks, crie um **Delta Live Tables Pipeline** em modo *Triggered* ou *Continuous*.
2. Aponte a biblioteca principal para o arquivo `pipelines/pipeline_financeiro.py`.
3. Garanta previamente a existência do catálogo `platfunc` e dos esquemas `aafn_ing`, `aafn_tgt` e `aafn_ddm`.
4. Configure os parâmetros opcionais via `spark.conf` no pipeline para ajustar fontes e janelas de dados:

| Chave | Descrição | Padrão |
|----|----|----|
| `techcare.b3.tickers` | Lista separada por vírgulas com os tickers da B3. | `PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3,ELET3,B3SA3` |
| `techcare.b3.start_date` / `techcare.b3.end_date` | Datas (YYYY-MM-DD) para histórico via Yahoo Finance. | `2015-01-01` / data atual |
| `techcare.bacen.series` | JSON com pares `{nome: código}` das séries SGS. | `{"selic":1178,"cdi":12,"ipca":433,"poupanca":195,"igpm":189,"inpc":188,"igpdi":190,"selic_meta":432}` |
| `techcare.bacen.start_date` / `techcare.bacen.end_date` | Intervalo de datas para as séries BACEN. | `2010-01-01` / data atual |
| `techcare.indices.futuros.config` | JSON com a configuração dos índices globais. | `INDICES_FUTUROS_PADRAO` |
| `techcare.catalogo.destino` | Catálogo Unity Catalog onde o pipeline criará as tabelas. | `platfunc` |
| `techcare.esquema.bronze` | Esquema da camada Bronze (ingestão). | `aafn_ing` |
| `techcare.esquema.prata` | Esquema da camada Prata (transformação). | `aafn_tgt` |
| `techcare.esquema.ouro` | Esquema da camada Ouro (data mart). | `aafn_ddm` |

### Boas práticas aplicadas

- Cada tabela possui comentários (`comment`) e validações de qualidade com `dlt.expect`.
- As tabelas Bronze acrescentam `ingestion_timestamp` para facilitar auditoria.
- As transformações utilizam APIs do Spark (em vez de Pandas) garantindo escalabilidade.
- O catálogo e os esquemas são validados antes da execução.
- As tabelas *gold* consolidam indicadores para B3, BACEN e **índices globais** com métricas padronizadas.

### Estrutura do Repositório

```text
techCare_TI_money/
├── README.md                        # Documentação do projeto e guia de configuração
├── requirements.txt                 # Dependências do projeto (yfinance, requests, etc.)
├── requirements-dev.txt             # Dependências de desenvolvimento e CI/CD
├── explorations/
│   └── teste_bronze.ipynb           # Notebooks de teste e exploração de dados
├── pipelines/
│   ├── __init__.py
│   └── pipeline_financeiro.py       # Ponto de entrada (Entry Point) do pipeline DLT
├── transformacoes/
│   ├── __init__.py
│   ├── bronze.py                    # Definição das tabelas Bronze (Ingestão bruta)
│   ├── prata.py                     # Definição das tabelas Prata (Limpeza e Refino)
│   └── ouro.py                      # Definição das tabelas Ouro (Agregados e KPIs)
└── utilitarios/
    ├── __init__.py
    ├── configuracoes.py             # Variáveis de ambiente, nomes de tabelas e taxonomia
    └── fontes_dados.py              # Funções de extração (APIs Yahoo Finance, BACEN, brapi.dev)
```


