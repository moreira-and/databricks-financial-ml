"""Conexões e funções de captura de dados externos utilizados pelo pipeline - Versão Refatorada com brapi.dev e sgs."""

from __future__ import annotations

import json
import logging
import os
import pandas as pd
import requests
import time

from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Any, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.dbutils import DBUtils


# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Adiciona um handler para o console com formatação detalhada
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Usa a sessão Spark existente do ambiente DLT
try:
    spark = SparkSession.active()
    logger.info("Sessão Spark obtida com sucesso do ambiente DLT")
except Exception as e:
    logger.error(f"Erro ao obter sessão Spark: {str(e)}")
    raise

# Importa biblioteca sgs para dados do BACEN
try:
    import sgs
    logger.info("Biblioteca sgs importada com sucesso")
except ModuleNotFoundError:
    logger.warning("Biblioteca sgs não encontrada. Instale com: pip install sgs")
    sgs = None


def _converter_data(
    data: str,
    formato_entrada: str = "%d/%m/%Y",
    formato_saida: str = "%Y-%m-%d",
) -> str:
    """
    Converte data entre diferentes formatos.
    
    Args:
        data: String representando a data
        formato_entrada: Formato da data de entrada (default: DD/MM/YYYY)
        formato_saida: Formato da data de saída (default: YYYY-MM-DD)
    
    Returns:
        Data convertida no formato de saída
    
    Raises:
        ValueError: Se a data estiver em formato inválido
    """
    try:
        return datetime.strptime(data, formato_entrada).strftime(formato_saida)
    except ValueError as e:
        raise ValueError(f"Formato de data inválido. Use {formato_entrada}. Erro: {str(e)}")


def buscar_historico_b3(
    tickers: Iterable[str],
    inicio: str,
    fim: str,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Busca histórico de preços dos tickers usando a API brapi.dev.
    
    LIMITAÇÕES DO PLANO BÁSICO:
    - 1 ativo por requisição (não múltiplos)
    - Dados históricos de apenas 3 meses
    - 15.000 requisições por mês
    
    Args:
        tickers: Lista de códigos de ações (ex: ["PETR4", "VALE3", "MGLU3"])
        inicio: Data inicial no formato DD/MM/YYYY
        fim: Data final no formato DD/MM/YYYY
        api_key: Token da API brapi (opcional, usa token padrão se não fornecido)
    
    Returns:
        DataFrame com histórico de preços
    
    Raises:
        ValueError: Se as datas estiverem em formato inválido
    """
    logger.info("=== INICIANDO BUSCA COM BRAPI.DEV ===")
    logger.info(f"Tickers solicitados: {list(tickers)}")
    logger.info(f"Período: {inicio} até {fim}")
    
    # Token da API
    dbutils = DBUtils(spark)
    BRAPI_TOKEN = dbutils.secrets.get("brapi_scope", "BRAPI_TOKEN")
    BASE_URL = "https://brapi.dev/api/quote"
    
    # Valida datas
    try:
        inicio_fmt = _converter_data(inicio)
        fim_fmt = _converter_data(fim)
    except ValueError as e:
        logger.error(f"Erro ao validar datas: {str(e)}")
        raise
    
    # Calcula diferença de dias
    data_inicio = datetime.strptime(inicio, "%d/%m/%Y")
    data_fim = datetime.strptime(fim, "%d/%m/%Y")
    dias_diferenca = (data_fim - data_inicio).days
    
    # LIMITAÇÃO: Plano básico só permite 3 meses (90 dias)
    if dias_diferenca > 90:
        logger.warning(
            f"⚠️ ATENÇÃO: Período solicitado ({dias_diferenca} dias) excede o limite do plano básico (90 dias)"
        )
        logger.warning("Ajustando para buscar apenas os últimos 3 meses")
        data_inicio = data_fim - timedelta(days=90)
        inicio_fmt = data_inicio.strftime("%Y-%m-%d")
        logger.info(f"Novo período: {data_inicio.strftime('%d/%m/%Y')} até {fim}")
    
    # Define range para API (3mo = 3 meses)
    range_api = "3mo"
    logger.info(f"Range calculado para API: {range_api}")
    
    colunas = [
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Dividends",
        "Stock_Splits",
        "ticker",
    ]
    
    quadros: List[pd.DataFrame] = []
    erros: List[str] = []
    avisos: List[str] = []
    
    if not tickers:
        logger.error("Nenhum ticker fornecido!")
        return pd.DataFrame(columns=colunas)
    
    tickers_list = list(tickers)
    total_tickers = len(tickers_list)
    
    logger.info(f"Total de tickers a processar: {total_tickers}")
    logger.info(
        f"Requisições estimadas: {total_tickers} (1 por ticker - limitação do plano básico)"
    )
    
    # LIMITAÇÃO: Processar 1 ticker por vez (plano básico não permite múltiplos)
    for idx, ticker in enumerate(tickers_list):
        try:
            ticker_base = ticker.replace(".SA", "").upper()
            logger.info(f"[{idx+1}/{total_tickers}] Processando: {ticker_base}")
            
            # Delay entre requisições para evitar rate limit
            if idx > 0:
                delay = 2  # 2 segundos entre requisições
                logger.info(f"Aguardando {delay}s antes da próxima requisição...")
                time.sleep(delay)
            
            # Monta URL para 1 ticker apenas (limitação do plano)
            url = f"{BASE_URL}/{ticker_base}"
            params = {
                "range": range_api,
                "interval": "1d",
                "fundamental": "false",
                "token": BRAPI_TOKEN,
            }
            
            logger.info(f"Requisição: GET {url}")
            logger.debug(f"Parâmetros: {params}")
            
            # Faz requisição com retry
            max_tentativas = 3
            tentativa = 0
            resposta = None
            
            while tentativa < max_tentativas:
                tentativa += 1
                try:
                    logger.info(f"Tentativa {tentativa}/{max_tentativas}")
                    resposta = requests.get(url, params=params, timeout=30)
                    
                    if resposta.status_code == 429:
                        logger.warning("Rate limit atingido. Aguardando 10s...")
                        time.sleep(10)
                        continue
                    
                    resposta.raise_for_status()
                    break
                    
                except requests.exceptions.HTTPError:
                    if resposta is not None and resposta.status_code == 429:
                        if tentativa < max_tentativas:
                            logger.warning(
                                "Rate limit. Tentando novamente em 10s..."
                            )
                            time.sleep(10)
                            continue
                        erro_msg = (
                            f"Rate limit excedido para {ticker_base} "
                            f"após {max_tentativas} tentativas"
                        )
                        logger.error(erro_msg)
                        erros.append(erro_msg)
                        break
                    raise
            
            if not resposta or resposta.status_code != 200:
                erros.append(f"Falha ao buscar {ticker_base}")
                continue
            
            dados = resposta.json()
            logger.debug(f"Resposta recebida: {len(str(dados))} caracteres")
            
            # Verifica estrutura da resposta
            if "results" not in dados or not dados["results"]:
                aviso = f"Nenhum dado retornado para {ticker_base}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue
            
            resultado = dados["results"][0]
            
            # Verifica se há dados históricos
            if (
                "historicalDataPrice" not in resultado
                or not resultado["historicalDataPrice"]
            ):
                aviso = f"Sem dados históricos para {ticker_base}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue
            
            historico_data = resultado["historicalDataPrice"]
            logger.info(f"Dados históricos encontrados: {len(historico_data)} registros")
            
            # Converte para DataFrame
            df_historico = pd.DataFrame(historico_data)
            
            # Converte timestamp para data
            df_historico["Date"] = pd.to_datetime(df_historico["date"], unit="s")
            
            # Renomeia colunas para padrão
            mapeamento_colunas = {
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
            df_historico = df_historico.rename(columns=mapeamento_colunas)
            
            # Adiciona colunas faltantes
            df_historico["Dividends"] = 0.0
            df_historico["Stock_Splits"] = 0.0
            df_historico["ticker"] = ticker_base
            
            # Seleciona apenas colunas necessárias
            df_historico = df_historico[colunas]
            
            # Filtra pelo período solicitado (se necessário)
            df_historico = df_historico[
                (df_historico["Date"] >= inicio_fmt)
                & (df_historico["Date"] <= fim_fmt)
            ]
            
            if df_historico.empty:
                aviso = f"Nenhum dado no período para {ticker_base}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue
            
            logger.info(f"✓ {ticker_base}: {len(df_historico)} registros processados")
            quadros.append(df_historico)
            
        except Exception as e:
            erro_msg = f"Erro ao processar {ticker}: {str(e)}"
            logger.error(erro_msg, exc_info=True)
            erros.append(erro_msg)
            continue
    
    # Relatório final
    if avisos:
        logger.warning(f"\n=== AVISOS DURANTE A BUSCA ({len(avisos)}) ===")
        for aviso in avisos:
            logger.warning(f"  - {aviso}")
    
    if erros:
        logger.error(f"\n=== ERROS DURANTE A BUSCA ({len(erros)}) ===")
        for erro in erros:
            logger.error(f"  - {erro}")
    
    if not quadros:
        logger.error("Nenhum dado encontrado para nenhum ticker!")
        return pd.DataFrame(columns=colunas)
    
    logger.info("\n=== RESUMO ===")
    logger.info(f"Tickers processados com sucesso: {len(quadros)}/{total_tickers}")
    
    resultado = pd.concat(quadros, ignore_index=True)
    
    # Garante tipos de dados corretos
    resultado["Date"] = pd.to_datetime(resultado["Date"], errors="coerce")
    resultado = resultado.dropna(subset=["Date"])
    
    colunas_numericas = [
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Dividends",
        "Stock_Splits",
    ]
    for col in colunas_numericas:
        resultado[col] = pd.to_numeric(resultado[col], errors="coerce")
        resultado[col] = resultado[col].fillna(0.0)
    
    logger.info(f"Total de registros obtidos: {len(resultado)}")
    logger.info(
        f"Período coberto: {resultado['Date'].min()} até {resultado['Date'].max()}"
    )
    logger.info("=== BUSCA FINALIZADA ===\n")
    
    return resultado


def criar_dataframe_vazio(schema: Any) -> DataFrame:
    """Cria um DataFrame vazio com o schema especificado."""
    return spark.createDataFrame([], schema)


# ============================================================================
# FUNÇÕES BACEN - REFATORADAS COM SGS
# ============================================================================

def buscar_series_bacen(
    series: Dict[str, int],
    inicio: str,
    fim: str,
) -> pd.DataFrame:
    """
    Busca séries temporais do BACEN usando a biblioteca sgs.
    
    Esta função substitui a implementação anterior com requests diretos,
    utilizando a biblioteca sgs que oferece:
    - Retry automático
    - Cache de requisições
    - Melhor tratamento de erros
    - Interface mais simples
    
    Args:
        series: Dicionário com nome da série como chave e código como valor
                Exemplo: {"IPCA": 433, "CDI": 12, "SELIC": 432}
        inicio: Data inicial no formato DD/MM/YYYY
        fim: Data final no formato DD/MM/YYYY
    
    Returns:
        DataFrame com colunas:
        - data: Data da observação
        - valor: Valor da série temporal
        - serie: Nome da série
    
    Raises:
        ValueError: Se as datas estiverem em formato inválido
        ModuleNotFoundError: Se a biblioteca sgs não estiver instalada
    """
    logger.info("=== INICIANDO BUSCA DE SÉRIES BACEN COM SGS ===")
    logger.info(f"Séries solicitadas: {list(series.keys())}")
    logger.info(f"Período: {inicio} até {fim}")
    
    # Verifica se a biblioteca sgs está disponível
    if sgs is None:
        erro_msg = (
            "Biblioteca sgs não encontrada. "
            "Instale com: pip install sgs"
        )
        logger.error(erro_msg)
        raise ModuleNotFoundError(erro_msg)
    
    # Valida formato das datas
    try:
        datetime.strptime(inicio, "%d/%m/%Y")
        datetime.strptime(fim, "%d/%m/%Y")
    except ValueError as e:
        erro_msg = f"Data deve estar no formato DD/MM/YYYY: {str(e)}"
        logger.error(erro_msg)
        raise ValueError(erro_msg)
    
    # Valida que há séries para buscar
    if not series:
        logger.warning("Nenhuma série fornecida!")
        return pd.DataFrame(columns=["data", "valor", "serie"])
    
    quadros: List[pd.DataFrame] = []
    erros: List[str] = []
    
    # Processa cada série individualmente
    for nome_serie, codigo_serie in series.items():
        try:
            logger.info(f"Buscando série '{nome_serie}' (código: {codigo_serie})")
            
            serie_temporal = sgs.time_serie(
                codigo_serie,
                start=inicio,
                end=fim,
            )
            
            # Verifica se retornou dados
            if serie_temporal is None or serie_temporal.empty:
                logger.warning(
                    f"Nenhum dado retornado para '{nome_serie}' (código {codigo_serie})"
                )
                erros.append(f"Nenhum dado retornado para {nome_serie}")
                continue
            
            # Converte Series do pandas para DataFrame
            df_serie = serie_temporal.to_frame(name="valor")
            df_serie = df_serie.reset_index()
            df_serie.columns = ["data", "valor"]
            
            # Adiciona nome da série
            df_serie["serie"] = nome_serie
            
            # Garante tipos corretos
            df_serie["data"] = pd.to_datetime(df_serie["data"], errors="coerce")
            df_serie["valor"] = pd.to_numeric(df_serie["valor"], errors="coerce")
            
            # Remove linhas com dados inválidos
            antes = len(df_serie)
            df_serie = df_serie.dropna(subset=["data", "valor"])
            depois = len(df_serie)
            
            if antes > depois:
                logger.warning(
                    f"Removidas {antes - depois} linhas com dados inválidos de '{nome_serie}'"
                )
            
            if df_serie.empty:
                logger.warning(f"Todos os dados de '{nome_serie}' eram inválidos")
                erros.append(f"Dados inválidos para série {nome_serie}")
                continue
            
            quadros.append(df_serie)
            logger.info(f"✓ {nome_serie}: {len(df_serie)} registros obtidos")
            
        except Exception as e:
            erro_msg = (
                f"Erro ao buscar '{nome_serie}' (código {codigo_serie}): {str(e)}"
            )
            logger.error(erro_msg)
            erros.append(erro_msg)
            continue
    
    # Reporta erros
    if erros:
        logger.warning(f"\n=== AVISOS DURANTE A BUSCA ({len(erros)}) ===")
        for erro in erros:
            logger.warning(f"  - {erro}")
    
    # Consolida resultados
    if not quadros:
        logger.error("Nenhum dado encontrado para nenhuma série!")
        return pd.DataFrame(columns=["data", "valor", "serie"])
    
    logger.info("\n=== CONSOLIDANDO RESULTADOS ===")
    resultado_final = pd.concat(quadros, ignore_index=True)
    
    # Ordenação final
    resultado_final = resultado_final.sort_values(["serie", "data"]).reset_index(
        drop=True
    )
    
    logger.info(f"✓ Total de registros obtidos: {len(resultado_final)}")
    logger.info(
        f"✓ Séries com dados: {resultado_final['serie'].nunique()}"
    )
    logger.info(
        f"✓ Período coberto: {resultado_final['data'].min()} até {resultado_final['data'].max()}"
    )
    logger.info("=== BUSCA CONCLUÍDA COM SUCESSO ===\n")
    
    return resultado_final


def buscar_multiplas_series_bacen(
    codigos_series: Union[List[int], Dict[str, int]],
    inicio: str,
    fim: str,
) -> pd.DataFrame:
    """
    Busca múltiplas séries do BACEN de forma otimizada usando sgs.dataframe.
    
    Esta função é mais eficiente quando você precisa buscar várias séries
    simultaneamente, pois usa sgs.dataframe que faz requisições em paralelo.
    
    Args:
        codigos_series: Lista de códigos ou dicionário {nome: código}
                       Exemplo: [433, 12, 432] ou {"IPCA": 433, "CDI": 12}
        inicio: Data inicial no formato DD/MM/YYYY
        fim: Data final no formato DD/MM/YYYY
    
    Returns:
        DataFrame com as séries em colunas (formato wide)
        - Index: Datas
        - Colunas: Códigos ou nomes das séries
    """
    logger.info("=== BUSCA OTIMIZADA DE MÚLTIPLAS SÉRIES BACEN ===")
    
    # Verifica se a biblioteca sgs está disponível
    if sgs is None:
        raise ModuleNotFoundError(
            "Biblioteca sgs não encontrada. Instale com: pip install sgs"
        )
    
    # Valida datas
    try:
        datetime.strptime(inicio, "%d/%m/%Y")
        datetime.strptime(fim, "%d/%m/%Y")
    except ValueError as e:
        raise ValueError(f"Data deve estar no formato DD/MM/YYYY: {str(e)}")
    
    try:
        # Se for dicionário, extrai os códigos
        if isinstance(codigos_series, dict):
            codigos = list(codigos_series.values())
            nomes = list(codigos_series.keys())
            logger.info(f"Buscando {len(codigos)} séries: {nomes}")
        else:
            codigos = codigos_series
            nomes = None
            logger.info(f"Buscando {len(codigos)} séries: {codigos}")
        
        # Usa sgs.dataframe para buscar múltiplas séries de uma vez
        df = sgs.dataframe(
            codigos,
            start=inicio,
            end=fim,
        )
        
        # Se temos nomes personalizados, renomeia as colunas
        if nomes:
            # Cria mapeamento de código para nome
            mapeamento = dict(zip(codigos, nomes))
            df.columns = [mapeamento.get(col, col) for col in df.columns]
        
        logger.info(f"✓ {len(df)} registros obtidos para {len(df.columns)} séries")
        logger.info(f"✓ Período: {df.index.min()} até {df.index.max()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Erro ao buscar séries: {str(e)}")
        raise


def buscar_indices_futuros(
    indices_config: Dict[str, Dict[str, str]],
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Busca cotações atuais de derivativos de índices globais usando a API brapi.dev.
    
    Mantém o schema original da API (camada bronze), enriquecendo com:
    - indice: identificador lógico (ex: IndSp500, IndBovespa)
    - regiao: região macro (US, BR, EU, APAC, MEA)
    - categoria: tipo de índice (equity_index, reit_index, volatility_index, etc.)
    
    Args:
        indices_config:
            Dicionário com configuração dos índices futuros:
            {
                "IndSp500": {"ticker": "^GSPC", "regiao": "US", "categoria": "equity_index"},
                ...
            }
        api_key:
            Token da API brapi (opcional, usa token padrão se não fornecido)
    
    Returns:
        DataFrame com uma linha por índice contendo:
        - currency, shortName, longName
        - regularMarketChange, regularMarketChangePercent
        - regularMarketTime, regularMarketPrice
        - regularMarketDayHigh, regularMarketDayLow, regularMarketDayRange
        - regularMarketPreviousClose, regularMarketOpen, regularMarketVolume
        - fiftyTwoWeekRange, fiftyTwoWeekLow, fiftyTwoWeekHigh
        - symbol, logourl
        - indice, regiao, categoria
    """
    logger.info("=== INICIANDO BUSCA DE ÍNDICES FUTUROS COM BRAPI.DEV ===")
    
    if not indices_config:
        logger.warning("Nenhum índice futuro configurado.")
        colunas = [
            "currency",
            "shortName",
            "longName",
            "regularMarketChange",
            "regularMarketChangePercent",
            "regularMarketTime",
            "regularMarketPrice",
            "regularMarketDayHigh",
            "regularMarketDayLow",
            "regularMarketDayRange",
            "regularMarketPreviousClose",
            "regularMarketOpen",
            "regularMarketVolume",
            "fiftyTwoWeekRange",
            "fiftyTwoWeekLow",
            "fiftyTwoWeekHigh",
            "symbol",
            "logourl",
            "indice",
            "regiao",
            "categoria",
        ]
        return pd.DataFrame(columns=colunas)
    
    BRAPI_TOKEN = api_key or "bskwmkRoxVSMKPwR5HHUSE"
    BASE_URL = "https://brapi.dev/api/quote"
    
    colunas = [
        "currency",
        "shortName",
        "longName",
        "regularMarketChange",
        "regularMarketChangePercent",
        "regularMarketTime",
        "regularMarketPrice",
        "regularMarketDayHigh",
        "regularMarketDayLow",
        "regularMarketDayRange",
        "regularMarketPreviousClose",
        "regularMarketOpen",
        "regularMarketVolume",
        "fiftyTwoWeekRange",
        "fiftyTwoWeekLow",
        "fiftyTwoWeekHigh",
        "symbol",
        "logourl",
        "indice",
        "regiao",
        "categoria",
    ]
    
    quadros: List[pd.DataFrame] = []
    erros: List[str] = []
    avisos: List[str] = []
    
    total_indices = len(indices_config)
    logger.info(f"Índices futuros configurados: {total_indices}")
    
    for idx, (nome_indice, meta) in enumerate(indices_config.items()):
        try:
            ticker = meta.get("ticker")
            regiao = meta.get("regiao")
            categoria = meta.get("categoria")
            
            if not ticker:
                logger.warning(f"Ticker não definido para índice {nome_indice}, ignorando.")
                continue
            
            logger.info(
                f"[{idx+1}/{total_indices}] Buscando {nome_indice} ({ticker}) "
                f"- Região: {regiao} - Categoria: {categoria}"
            )
            
            if idx > 0:
                # Atraso simples para evitar rate limit
                time.sleep(2)
            
            url = f"{BASE_URL}/{ticker}"
            params = {"token": BRAPI_TOKEN}
            
            max_tentativas = 3
            tentativa = 0
            resposta = None
            
            while tentativa < max_tentativas:
                tentativa += 1
                try:
                    logger.info(f"Tentativa {tentativa}/{max_tentativas} para {nome_indice}")
                    resposta = requests.get(url, params=params, timeout=30)
                    
                    if resposta.status_code == 429:
                        logger.warning("Rate limit atingido. Aguardando 10s...")
                        time.sleep(10)
                        continue
                    
                    resposta.raise_for_status()
                    break
                except requests.exceptions.HTTPError:
                    if resposta is not None and resposta.status_code == 429:
                        if tentativa < max_tentativas:
                            logger.warning("Rate limit. Tentando novamente em 10s...")
                            time.sleep(10)
                            continue
                        erro_msg = (
                            f"Rate limit excedido para {nome_indice} "
                            f"após {max_tentativas} tentativas"
                        )
                        logger.error(erro_msg)
                        erros.append(erro_msg)
                        break
                    raise
            
            if not resposta or resposta.status_code != 200:
                erros.append(f"Falha ao buscar {nome_indice}")
                continue
            
            dados = resposta.json()
            if "results" not in dados or not dados["results"]:
                aviso = f"Nenhum dado retornado para {nome_indice}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue
            
            resultado = dados["results"][0]
            
            # Monta registro alinhado ao schema original da API + taxonomia
            registro = {
                "currency": resultado.get("currency"),
                "shortName": resultado.get("shortName"),
                "longName": resultado.get("longName"),
                "regularMarketChange": resultado.get("regularMarketChange"),
                "regularMarketChangePercent": resultado.get(
                    "regularMarketChangePercent"
                ),
                "regularMarketTime": resultado.get("regularMarketTime"),
                "regularMarketPrice": resultado.get("regularMarketPrice"),
                "regularMarketDayHigh": resultado.get("regularMarketDayHigh"),
                "regularMarketDayLow": resultado.get("regularMarketDayLow"),
                "regularMarketDayRange": resultado.get("regularMarketDayRange"),
                "regularMarketPreviousClose": resultado.get(
                    "regularMarketPreviousClose"
                ),
                "regularMarketOpen": resultado.get("regularMarketOpen"),
                "regularMarketVolume": resultado.get("regularMarketVolume"),
                "fiftyTwoWeekRange": resultado.get("fiftyTwoWeekRange"),
                "fiftyTwoWeekLow": resultado.get("fiftyTwoWeekLow"),
                "fiftyTwoWeekHigh": resultado.get("fiftyTwoWeekHigh"),
                "symbol": resultado.get("symbol"),
                "logourl": resultado.get("logourl"),
                "indice": nome_indice,
                "regiao": regiao,
                "categoria": categoria,
            }
            
            df_indice = pd.DataFrame([registro])
            quadros.append(df_indice)
            
            logger.info(
                f"✓ {nome_indice}: preço {registro['regularMarketPrice']} "
                f"{registro['currency']}"
            )
        
        except Exception as e:
            erro_msg = f"Erro ao processar índice {nome_indice}: {str(e)}"
            logger.error(erro_msg, exc_info=True)
            erros.append(erro_msg)
            continue
    
    if avisos:
        logger.warning(f"\n=== AVISOS DURANTE A BUSCA ({len(avisos)}) ===")
        for aviso in avisos:
            logger.warning(f"  - {aviso}")
    
    if erros:
        logger.error(f"\n=== ERROS DURANTE A BUSCA ({len(erros)}) ===")
        for erro in erros:
            logger.error(f"  - {erro}")
    
    if not quadros:
        logger.error("Nenhum dado obtido para índices futuros.")
        return pd.DataFrame(columns=colunas)
    
    resultado = pd.concat(quadros, ignore_index=True)
    
    # Conversão básica de tipos
    if "regularMarketTime" in resultado.columns:
        resultado["regularMarketTime"] = pd.to_datetime(
            resultado["regularMarketTime"], errors="coerce"
        )
    
    colunas_numericas = [
        "regularMarketChange",
        "regularMarketChangePercent",
        "regularMarketPrice",
        "regularMarketDayHigh",
        "regularMarketDayLow",
        "regularMarketPreviousClose",
        "regularMarketOpen",
        "regularMarketVolume",
        "fiftyTwoWeekLow",
        "fiftyTwoWeekHigh",
    ]
    for col in colunas_numericas:
        if col in resultado.columns:
            resultado[col] = pd.to_numeric(resultado[col], errors="coerce")
    
    logger.info(
        f"Total de índices futuros obtidos: {len(resultado)} "
        f"({resultado['indice'].nunique()} identificadores lógicos)"
    )
    
    return resultado[colunas]


__all__ = [
    "BrapiClient",
    "buscar_historico_b3",
    "buscar_series_bacen",
    "buscar_multiplas_series_bacen",
    "buscar_indices_futuros",
    "criar_dataframe_vazio",
]
