"""Pacote das transformações Bronze, Prata e Ouro."""

from importlib import import_module
from pathlib import Path
import sys

# Em execuções de notebooks no Databricks, ``__file__`` pode não estar
# disponível. Assim, detectamos o diretório de "transformacoes" usando um
# fallback baseado no diretório de trabalho atual.
try:  # pragma: no cover - caminho alternativo apenas no Databricks
    _diretorio_atual = Path(__file__).resolve().parent
except NameError:  # pragma: no cover - notebooks não expõem ``__file__``
    _diretorio_atual = Path.cwd()
    if _diretorio_atual.name != "transformacoes":
        candidato = _diretorio_atual / "transformacoes"
        if candidato.exists():
            _diretorio_atual = candidato
        else:
            raise RuntimeError(
                "Não foi possível localizar o diretório 'transformacoes'. "
                "Garanta que o notebook esteja dentro da pasta do pipeline."
            )

# Mantém o pacote importável tanto por "transformacoes" quanto por
# "databricks.transformacoes" sem executar as camadas automaticamente.
__path__ = [str(_diretorio_atual)]
sys.modules.setdefault("databricks.transformacoes", sys.modules[__name__])

_ALIAS_VALIDOS = {"bronze", "prata", "ouro"}


def __getattr__(nome: str):  # pragma: no cover - acesso dinâmico
    if nome in _ALIAS_VALIDOS:
        modulo = import_module(f"{__name__}.{nome}")
        setattr(sys.modules[__name__], nome, modulo)
        return modulo
    raise AttributeError(f"módulo '{__name__}' não possui atributo '{nome}'")


__all__ = sorted(_ALIAS_VALIDOS)
