"""Configurações, nomenclaturas e utilitários básicos do pipeline DLT."""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

# Definição dos esquemas para cada camada
ESQUEMAS = {
    "bronze": "aafn_ing",  # Ingestão - Dados brutos
    "prata": "aafn_tgt",   # Target - Dados normalizados
    "ouro": "aafn_ddm"     # Data Mart - Dados analíticos
}

# Qualidade e propriedades por camada
PROPRIEDADES_TABELAS = {
    "bronze": {
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "pipelines.trigger.interval": "12 hours",  # Atualização a cada 12 horas
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    },
    "prata": {
        "quality": "silver",
        "pipelines.reset.allowed": "false",
        "pipelines.trigger.interval": "12 hours",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    },
    "ouro": {
        "quality": "gold",
        "pipelines.reset.allowed": "false",
        "pipelines.trigger.interval": "12 hours",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true"
    }
}

# Estrutura das tabelas por camada e domínio
DEFINICAO_TABELAS = {
    "bronze": {
        # Dados brutos - Camada de Ingestão (ing)
        "cotacoes_b3": {
            "descricao": "Cotações brutas da B3 (Yahoo Finance)",
            "alias": "Cotações B3",
            "nome_fisico": "cotacoes_b3",
            "colunas_esperadas": [
                "Date",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Dividends",
                "Stock_Splits",
                "ticker",
            ],
        },
        "series_bacen": {
            "descricao": "Séries temporais do BACEN (SGS)",
            "alias": "Indicadores BACEN",
            "nome_fisico": "series_bacen",
            "colunas_esperadas": ["data", "valor", "serie"],
        },
        # NOVA TABELA: dados brutos de índices globais com HISTÓRICO (via brapi.dev)
        "indices_futuros": {
            "descricao": "Histórico de índices globais (brapi.dev - historicalDataPrice)",
            "alias": "Índices Globais",
            "nome_fisico": "indices_futuros",
            # Schema base da API + campos do histórico e metadados de taxonomia
            "colunas_esperadas": [
                # Metadados do índice
                "symbol",
                "currency",
                "shortName",
                "longName",
                "logourl",
                "indice",
                "regiao",
                "categoria",
                "fiftyTwoWeekLow",
                "fiftyTwoWeekHigh",
                # Histórico diário (historicalDataPrice)
                "date",           # timestamp unix (segundos)
                "open",
                "high",
                "low",
                "close",
                "volume",
                "adjustedClose",
                # Derivados para facilitar consumo
                "trade_date",     # data do pregão (yyy-MM-dd)
            ],
        },
    },
    "prata": {
        # Dados normalizados - Camada de Transformação (tgt)
        "tb_mkt_eqt_day": {
            "descricao": "Série histórica diária de equity",
            "alias": "Equity Diário",
            "nome_fisico": "tb_mkt_eqt_day",
            "fonte": "cotacoes_b3",
            "colunas_esperadas": [
                "trade_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "div_cash",
                "split_ratio",
                "symbol",
            ],
        },
        "tb_mkt_idx_eco": {
            "descricao": "Indicadores econômicos padronizados",
            "alias": "Índices Econômicos",
            "nome_fisico": "tb_mkt_idx_eco",
            "fonte": "series_bacen",
            "colunas_esperadas": ["ref_date", "idx_value", "idx_type", "frequency"],
        },
        # NOVA TABELA: série diária de índices globais normalizada
        "tb_mkt_idx_fut_day": {
            "descricao": "Série histórica diária de índices globais",
            "alias": "Índices Globais Diário",
            "nome_fisico": "tb_mkt_idx_fut_day",
            "fonte": "indices_futuros",
            "colunas_esperadas": [
                "trade_datetime",
                "trade_date",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "prev_close_price",
                "abs_change",
                "pct_change",
                "volume",
                "symbol",
                "index_symbol",
                "index_name",
                "region",
                "category",
                "currency",
                "price_52w_high",
                "price_52w_low",
            ],
        },
    },
    "ouro": {
        # Dados analíticos - Camada de Data Mart (ddm)
        "tb_mkt_eqt_perf": {
            "descricao": "Análise de performance de equity",
            "alias": "Performance de Ativos",
            "nome_fisico": "tb_mkt_eqt_perf",
            "fonte": "tb_mkt_eqt_day",
            "colunas_esperadas": [
                "symbol",
                "monthly_return",
                "volatility",
                "avg_price",
                "avg_volume",
                "last_update",
            ],
        },
        "tb_mkt_idx_dash": {
            "descricao": "Dashboard de indicadores macroeconômicos",
            "alias": "Dashboard Macro",
            "nome_fisico": "tb_mkt_idx_dash",
            "fonte": "tb_mkt_idx_eco",
            "colunas_esperadas": [
                "idx_type",
                "current_value",
                "mtd_change",
                "ytd_change",
                "trend_6m",
                "last_update",
            ],
        },
        # NOVA TABELA: performance de índices globais
        "tb_mkt_idx_fut_perf": {
            "descricao": "Análise de performance de índices globais",
            "alias": "Performance Índices Globais",
            "nome_fisico": "tb_mkt_idx_fut_perf",
            "fonte": "tb_mkt_idx_fut_day",
            "colunas_esperadas": [
                "symbol",
                "index_name",
                "region",
                "category",
                "currency",
                "monthly_return",
                "ytd_return",
                "volatility",
                "avg_price_30d",
                "last_close",
                "price_52w_high",
                "price_52w_low",
                "pct_from_52w_high",
                "pct_from_52w_low",
                "last_update",
            ],
        },
    },
}

# Taxonomia padrão para índices globais
INDICES_FUTUROS_PADRAO: Dict[str, Dict[str, str]] = {
    # América do Norte
    "IndSp500": {"ticker": "^GSPC", "regiao": "US", "categoria": "equity_index"},
    "IndNasdaq": {"ticker": "^IXIC", "regiao": "US", "categoria": "equity_index"},
    "IndDji": {"ticker": "^DJI", "regiao": "US", "categoria": "equity_index"},
    "IndRussell2000": {"ticker": "^RUT", "regiao": "US", "categoria": "equity_index"},
    "IndNyComposite": {"ticker": "^NYA", "regiao": "US", "categoria": "equity_index"},
    "IndAmex": {"ticker": "^XAX", "regiao": "US", "categoria": "equity_index"},
    "IndVix": {"ticker": "^VIX", "regiao": "US", "categoria": "volatility_index"},
    "IndTSX": {"ticker": "^GSPTSE", "regiao": "CA", "categoria": "equity_index"},

    # Brasil / América Latina
    "IndBovespa": {"ticker": "^BVSP", "regiao": "BR", "categoria": "equity_index"},
    "IndIFIX": {"ticker": "IFIX.SA", "regiao": "BR", "categoria": "reit_index"},
    "IndIPSA": {"ticker": "^IPSA", "regiao": "CL", "categoria": "equity_index"},
    # REMOVIDO IndCaso30 (estava dando 404)
    "IndMerval": {"ticker": "^MERV", "regiao": "AR", "categoria": "equity_index"},
    "IndIPC": {"ticker": "^MXX", "regiao": "MX", "categoria": "equity_index"},

    # Europa
    "IndFTSE100": {"ticker": "^FTSE", "regiao": "UK", "categoria": "equity_index"},
    "IndBUK100P": {"ticker": "^BUK100P", "regiao": "UK", "categoria": "equity_index"},
    "IndDax": {"ticker": "^GDAXI", "regiao": "DE", "categoria": "equity_index"},
    "IndEuroStoxx50": {"ticker": "^STOXX50E", "regiao": "EU", "categoria": "equity_index"},
    "IndN100": {"ticker": "^N100", "regiao": "EU", "categoria": "equity_index"},
    "IndBFX": {"ticker": "^BFX", "regiao": "BE", "categoria": "equity_index"},
    "IndCAC40": {"ticker": "^FCHI", "regiao": "FR", "categoria": "equity_index"},

    # Ásia-Pacífico
    "IndAllOrd": {"ticker": "^AORD", "regiao": "APAC", "categoria": "equity_index"},
    "IndASX200": {"ticker": "^AXJO", "regiao": "APAC", "categoria": "equity_index"},
    "IndNZ50": {"ticker": "^NZ50", "regiao": "APAC", "categoria": "equity_index"},
    "IndSTI": {"ticker": "^STI", "regiao": "APAC", "categoria": "equity_index"},
    "IndTWII": {"ticker": "^TWII", "regiao": "APAC", "categoria": "equity_index"},
    "IndBSE": {"ticker": "^BSESN", "regiao": "APAC", "categoria": "equity_index"},
    "IndJakarta": {"ticker": "^JKSE", "regiao": "APAC", "categoria": "equity_index"},
    "IndKospi": {"ticker": "^KS11", "regiao": "APAC", "categoria": "equity_index"},
    "IndKLSE": {"ticker": "^KLSE", "regiao": "APAC", "categoria": "equity_index"},
    "IndNikkei": {"ticker": "^N225", "regiao": "APAC", "categoria": "equity_index"},
    "IndHangSeng": {"ticker": "^HSI", "regiao": "APAC", "categoria": "equity_index"},

    # Oriente Médio e África
    "IndTA125": {"ticker": "^TA125.TA", "regiao": "MEA", "categoria": "equity_index"},
    "IndJSE": {"ticker": "^JN0U.JO", "regiao": "MEA", "categoria": "equity_index"},
}

# Cache de configurações para ambiente local
_config_local: Dict[str, str] = {}


def obter_configuracao(chave: str, padrao: Optional[str] = None) -> Optional[str]:
    """
    Obtém parâmetros de configuração na seguinte ordem de prioridade:
    1. Variável de ambiente
    2. Cache local
    3. Valor padrão
    """
    # Tenta variável de ambiente primeiro (converte chave.nested para CHAVE_NESTED)
    env_key = chave.replace(".", "_").upper()
    valor = os.environ.get(env_key)
    
    # Se não encontrou na env, tenta no cache local
    if valor is None:
        valor = _config_local.get(chave)
        
    # Retorna valor encontrado ou padrão
    return valor if valor is not None else padrao


def obter_lista_configuracoes(chave: str, padrao: str = "") -> List[str]:
    """Converte valores de configuração separados por vírgula em uma lista sanitizada."""
    valor = obter_configuracao(chave, padrao) or ""
    return [item.strip() for item in valor.split(",") if item.strip()]


def definir_configuracao_local(chave: str, valor: str) -> None:
    """Define uma configuração no cache local (útil para desenvolvimento e testes)."""
    _config_local[chave] = valor


def obter_nome_tabela(camada: str, nome_base: str, incluir_esquema: bool = True) -> str:
    """
    Retorna o nome completo da tabela para a camada especificada.
    
    Args:
        camada: Nome da camada ('bronze', 'prata' ou 'ouro')
        nome_base: Nome da tabela ou sua chave em DEFINICAO_TABELAS
        incluir_esquema: Se True, retorna o nome completo com esquema (ex: aafn_ing.tabela)
    
    Returns:
        Nome da tabela, opcionalmente prefixado com o esquema
    """
    if camada not in ESQUEMAS:
        raise ValueError(f"Camada inválida: {camada}. Use: bronze, prata ou ouro")
    
    # Primeiro tenta encontrar a tabela diretamente em DEFINICAO_TABELAS
    camada_def = DEFINICAO_TABELAS[camada]
    if nome_base in camada_def:
        nome_tabela = nome_base
    else:
        # Se não encontrou, procura em todas as chaves por um valor correspondente
        tabelas_encontradas = [
            (chave, meta)
            for chave, meta in camada_def.items()
            if meta.get("nome_fisico", chave) == nome_base
        ]
        
        if not tabelas_encontradas:
            raise ValueError(
                f"Tabela '{nome_base}' não encontrada na camada {camada}. "
                f"Tabelas disponíveis: {list(camada_def.keys())}"
            )
        nome_tabela = tabelas_encontradas[0][0]
    
    # Retorna com ou sem esquema
    if incluir_esquema:
        return f"{ESQUEMAS[camada]}.{nome_tabela}"
    return nome_tabela


def obter_metadados_tabela(camada: str, nome_base: str) -> Dict[str, Any]:
    """Retorna os metadados de uma tabela específica."""
    if camada not in DEFINICAO_TABELAS:
        raise ValueError(f"Camada inválida: {camada}")
        
    if nome_base not in DEFINICAO_TABELAS[camada]:
        raise ValueError(f"Tabela '{nome_base}' não encontrada na camada {camada}")
        
    return DEFINICAO_TABELAS[camada][nome_base]


def timestamp_ingestao() -> datetime:
    """Retorna o instante UTC da captura de dados para rastreabilidade."""
    return datetime.utcnow()


# Exporta apenas o necessário
__all__ = [
    "DEFINICAO_TABELAS",
    "ESQUEMAS",
    "PROPRIEDADES_TABELAS",
    "INDICES_FUTUROS_PADRAO",
    "obter_configuracao",
    "obter_lista_configuracoes",
    "definir_configuracao_local",
    "obter_nome_tabela",
    "obter_metadados_tabela",
    "timestamp_ingestao",
]
