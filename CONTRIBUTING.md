# Guia de Contribuicao

Este repositorio contem um pipeline Delta Live Tables (DLT) para extracao e transformacao de dados financeiros (B3, BACEN e indices globais via brapi.dev), organizado nas camadas Bronze, Prata e Ouro.

## Passo a passo (primeira vez no Databricks)

1. Garanta acesso ao workspace
Solicite ao time o URL do workspace Databricks e as permissoes para criar Repos e pipelines DLT.

2. Crie o Repo dentro do Databricks
No menu esquerdo, acesse `Repos` e clique em `Add Repo`. Cole a URL Git deste repositorio, escolha a branch e confirme.

3. Verifique a estrutura obrigatoria no workspace
A estrutura precisa conter exatamente estas pastas na raiz do Repo:
`pipelines/`, `transformacoes/`, `utilitarios/`.

4. Prepare o catalogo e os esquemas (Unity Catalog)
Se ainda nao existirem, crie o catalogo e os esquemas usados pelo pipeline (ajuste se seu ambiente usar outros nomes):

```sql
CREATE CATALOG IF NOT EXISTS platfunc;
CREATE SCHEMA IF NOT EXISTS platfunc.aafn_ing;
CREATE SCHEMA IF NOT EXISTS platfunc.aafn_tgt;
CREATE SCHEMA IF NOT EXISTS platfunc.aafn_ddm;
```

Se voce nao tiver permissao, solicite ao administrador.

5. Crie o pipeline Delta Live Tables
No menu esquerdo, va em `Data Engineering` > `Delta Live Tables` > `Create Pipeline`.
Configure:
- Name: um nome descritivo.
- Source code: `pipelines/pipeline_financeiro.py`.
- Target: `platfunc` (ou o catalogo usado no seu workspace).
- Storage: defina um local valido do workspace.
- Mode: `Triggered` para execucoes manuais (recomendado para desenvolvimento).

6. Configure parametros do pipeline (opcional)
No campo de configuracao do pipeline (Spark config), adicione chaves conforme necessidade:

| Chave | Descricao | Padrao |
| ---- | ---- | ---- |
| `techcare.b3.tickers` | Lista de tickers da B3 separados por virgula. | `PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3,ELET3,B3SA3` |
| `techcare.b3.start_date` / `techcare.b3.end_date` | Periodo para Yahoo Finance (YYYY-MM-DD). | `2015-01-01` / data atual |
| `techcare.bacen.series` | JSON com pares `{nome: codigo}` das series SGS. | `{"selic":1178,"cdi":12,"ipca":433,"poupanca":195,"igpm":189,"inpc":188,"igpdi":190,"selic_meta":432}` |
| `techcare.bacen.start_date` / `techcare.bacen.end_date` | Periodo das series BACEN. | `2010-01-01` / data atual |
| `techcare.indices.futuros.config` | JSON com configuracao dos indices globais. | `INDICES_FUTUROS_PADRAO` |
| `techcare.catalogo.destino` | Catalogo de destino das tabelas. | `platfunc` |
| `techcare.esquema.bronze` | Esquema da camada Bronze. | `aafn_ing` |
| `techcare.esquema.prata` | Esquema da camada Prata. | `aafn_tgt` |
| `techcare.esquema.ouro` | Esquema da camada Ouro. | `aafn_ddm` |

7. Execute e valide
Clique em `Start` no pipeline e acompanhe a criacao das tabelas nas camadas Bronze, Prata e Ouro.

## Passo a passo (contribuicao no codigo)

1. Crie uma branch de trabalho

```bash
git checkout -b feature/minha-feature
```

2. Edite os arquivos do pipeline
Principais pontos de edicao:
- `pipelines/pipeline_financeiro.py` (entry point DLT)
- `transformacoes/bronze.py`, `transformacoes/prata.py`, `transformacoes/ouro.py`
- `utilitarios/configuracoes.py`, `utilitarios/fontes_dados.py`

3. Configure o ambiente local (para validacoes)

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-dev.txt
pre-commit install
```

4. Rode as verificacoes locais

```bash
black transformacoes/ pipelines/ utilitarios/
isort transformacoes/ pipelines/ utilitarios/
flake8 transformacoes/ pipelines/ utilitarios/
pytest -v
```

5. Atualize o Repo no Databricks
No Databricks, dentro de `Repos`, use `Pull` para trazer sua branch ou clique em `Git` para selecionar a branch correta.

6. Teste no Databricks
Execute o pipeline DLT novamente e valide as tabelas e logs.

7. Commit e PR

```bash
git add .
git commit -m "feat: descreva resumidamente a mudanca"
git push origin feature/minha-feature
```

Abra um Pull Request no GitHub usando o template padrao e descreva como validar sua mudanca.

## Padroes de codigo

- Formatacao automatica com Black (linha maxima 100 colunas).
- Imports organizados com isort (perfil Black).
- Nomes em snake_case para funcoes e variaveis.
- Nomes em PascalCase para classes.
- Funcoes e modulos pequenos e claros.

## Testes

Sempre que possivel, adicione ou atualize testes. Antes de abrir o PR, garanta:

```bash
pytest -v
```

## Dicas gerais

- Prefira varios pequenos PRs a um PR muito grande.
- Documente no PR como testar sua mudanca.
- Em caso de duvida, abra uma issue ou pergunte no PR.
