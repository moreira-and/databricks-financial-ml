# Guia de Contribuição

Obrigado por contribuir com este projeto.

## 1. Requisitos

- Python 3.10+
- Git
- (Opcional, mas recomendado) VSCode

## 2. Configuração do ambiente

Na raiz do projeto:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate     # Windows

pip install -r requirements-dev.txt
pre-commit install
```

## 3. Fluxo básico de trabalho
- Criar uma branch a partir de main ou develop:
```bash
git checkout -b feature/minha-feature
```
- Fazer as alterações de código.
- Rodar as verificações locais:

# Formatação e imports
```
black transformacoes/ pipelines/ utilitarios/
isort transformacoes/ pipelines/ utilitarios/

# Lint
flake8 transformacoes/ pipelines/ utilitarios/

# Testes (se existirem)
pytest -v

```
## 4 Commitar:
```
git add .
git commit -m "feat: descreva resumidamente a mudança"

# Fazer push da branch:
git push origin feature/minha-feature

```
## Abrir um Pull Request no GitHub usando o template padrão.

## 5. Padrões de código
Formatação automática com Black (linha máxima 100 colunas).
Imports organizados com isort (perfil Black).
Nomes em snake_case para funções e variáveis.
Nomes em PascalCase para classes.
Mantenha funções e módulos pequenos e claros.

6. Testes
Sempre que possível, adicionar ou atualizar testes.
Antes de abrir PR, garantir que os testes passam:

```
pytest -v
```

8. Dicas gerais
Prefira vários pequenos PRs a um PR muito grande.
Documente no PR como testar sua mudança.
Em caso de dúvida, abra uma issue ou pergunte no PR.
