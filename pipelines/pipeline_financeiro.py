"""Pipeline principal do Delta Live Tables para o domínio financeiro."""

from importlib import import_module
from pathlib import Path
import sys

RAIZ_REPO = Path(__file__).resolve().parents[2]

for caminho in (RAIZ_REPO, RAIZ_REPO / "databricks"):
    texto = str(caminho)
    if texto not in sys.path:
        sys.path.append(texto)

bronze = import_module("transformacoes.bronze")
prata = import_module("transformacoes.prata")
ouro = import_module("transformacoes.ouro")

sys.modules.setdefault("databricks.transformacoes.bronze", bronze)
sys.modules.setdefault("databricks.transformacoes.prata", prata)
sys.modules.setdefault("databricks.transformacoes.ouro", ouro)

__all__ = ["bronze", "prata", "ouro"]
