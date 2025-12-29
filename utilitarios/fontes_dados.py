"""Conexões e funções de captura de dados externos utilizados pelo pipeline
- Versão Refatorada com brapi.dev e sgs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
import requests
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Adiciona um handler para o console com formatação detalhada
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(console_handler)

# Usa a sessão Spark existente do ambiente DLT
try:
    spark = SparkSession.active()
    logger.info("Sessão Spark obtida com sucesso do ambiente DLT")
except Exception as e:
    logger.error("Erro ao obter sessão Spark: %s", str(e))
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
    """
    try:
        return datetime.strptime(data, formato_entrada).strftime(formato_saida)
    except ValueError as e:
        raise ValueError(
            f"Formato de data inválido. Use {formato_entrada}. Erro: {str(e)}"
        ) from e


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
    """
    logger.info("=== INICIANDO BUSCA COM BRAPI.DEV ===")
    logger.info("Tickers solicitados: %s", list(tickers))
    logger.info("Período: %s até %s", inicio, fim)

    # Token da API
    dbutils = DBUtils(spark)
    brapi_token = api_key or dbutils.secrets.get("brapi_scope", "BRAPI_TOKEN")
    base_url = "https://brapi.dev/api/quote"

    # Valida datas
    try:
        inicio_fmt = _converter_data(inicio)
        fim_fmt = _converter_data(fim)
    except ValueError as e:
        logger.error("Erro ao validar datas: %s", str(e))
        raise

    # Calcula diferença de dias
    data_inicio = datetime.strptime(inicio, "%d/%m/%Y")
    data_fim = datetime.strptime(fim, "%d/%m/%Y")
    dias_diferenca = (data_fim - data_inicio).days

    # LIMITAÇÃO: Plano básico só permite 3 meses (90 dias)
    if dias_diferenca > 90:
        logger.warning(
            "⚠️ ATENÇÃO: Período solicitado (%d dias) excede o limite do "
            "plano básico (90 dias)",
            dias_diferenca,
        )
        logger.warning("Ajustando para buscar apenas os últimos 3 meses")
        data_inicio = data_fim - timedelta(days=90)
        inicio_fmt = data_inicio.strftime("%Y-%m-%d")
        logger.info(
            "Novo período: %s até %s",
            data_inicio.strftime("%d/%m/%Y"),
            fim,
        )

    # Define range para API (3mo = 3 meses)
    range_api = "3mo"
    logger.info("Range calculado para API: %s", range_api)

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

    logger.info("Total de tickers a processar: %d", total_tickers)
    logger.info(
        "Requisições estimadas: %d (1 por ticker - limitação do plano básico)",
        total_tickers,
    )

    # LIMITAÇÃO: Processar 1 ticker por vez (plano básico não permite múltiplos)
    max_tentativas = 3
    for idx, ticker in enumerate(tickers_list):
        try:
            ticker_base = ticker.replace(".SA", "").upper()
            logger.info("[%d/%d] Processando: %s", idx + 1, total_tickers, ticker_base)

            # Delay entre requisições para evitar rate limit
            if idx > 0:
                delay = 2
                logger.info("Aguardando %ds antes da próxima requisição...", delay)
                time.sleep(delay)

            url = f"{base_url}/{ticker_base}"
            params = {
                "range": range_api,
                "interval": "1d",
                "fundamental": "false",
                "token": brapi_token,
            }

            logger.info("Requisição: GET %s", url)
            logger.debug("Parâmetros: %s", params)

            tentativa = 0
            resposta: Optional[requests.Response] = None

            while tentativa < max_tentativas:
                tentativa += 1
                try:
                    logger.info("Tentativa %d/%d", tentativa, max_tentativas)
                    resposta = requests.get(url, params=params, timeout=30)

                    if resposta.status_code == 429:
                        if tentativa < max_tentativas:
                            logger.warning(
                                "Rate limit atingido. Aguardando 10s..."
                            )
                            time.sleep(10)
                            continue

                        erro_msg = (
                            "Rate limit excedido para "
                            f"{ticker_base} após {max_tentativas} tentativas"
                        )
                        logger.error(erro_msg)
                        erros.append(erro_msg)
                        resposta = None
                        break

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
                            "Rate limit excedido para "
                            f"{ticker_base} após {max_tentativas} tentativas"
                        )
                        logger.error(erro_msg)
                        erros.append(erro_msg)
                        break
                    raise

            if not resposta or resposta.status_code != 200:
                erros.append(f"Falha ao buscar {ticker_base}")
                continue

            dados = resposta.json()
            logger.debug("Resposta recebida: %d caracteres", len(str(dados)))

            if "results" not in dados or not dados["results"]:
                aviso = f"Nenhum dado retornado para {ticker_base}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue

            resultado = dados["results"][0]

            if (
                "historicalDataPrice" not in resultado
                or not resultado["historicalDataPrice"]
            ):
                aviso = f"Sem dados históricos para {ticker_base}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue

            historico_data = resultado["historicalDataPrice"]
            logger.info(
                "Dados históricos encontrados: %d registros",
                len(historico_data),
            )

            df_historico = pd.DataFrame(historico_data)

            df_historico["Date"] = pd.to_datetime(
                df_historico["date"],
                unit="s",
                errors="coerce",
            )

            mapeamento_colunas = {
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            }
            df_historico = df_historico.rename(columns=mapeamento_colunas)

            df_historico["Dividends"] = 0.0
            df_historico["Stock_Splits"] = 0.0
            df_historico["ticker"] = ticker_base

            df_historico = df_historico[colunas]

            df_historico = df_historico[
                (df_historico["Date"] >= inicio_fmt)
                & (df_historico["Date"] <= fim_fmt)
            ]

            if df_historico.empty:
                aviso = f"Nenhum dado no período para {ticker_base}"
                logger.warning(aviso)
                avisos.append(aviso)
                continue

            logger.info(
                "✓ %s: %d registros processados",
                ticker_base,
                len(df_historico),
            )
            quadros.append(df_historico)

        except Exception as e:
            erro_msg = f"Erro ao processar {ticker}: {str(e)}"
            logger.error(erro_msg, exc_info=True)
            erros.append(erro_msg)
            continue

    if avisos:
        logger.warning("=== AVISOS DURANTE A BUSCA (%d) ===", len(avisos))
        for aviso in avisos:
            logger.warning("  - %s", aviso)

    if erros:
        logger.error("=== ERROS DURANTE A BUSCA (%d) ===", len(erros))
        for erro in erros:
            logger.error("  - %s", erro)

    if not quadros:
        logger.error("Nenhum dado encontrado para nenhum ticker!")
        return pd.DataFrame(columns=colunas)

    logger.info("=== RESUMO ===")
    logger.info("Tickers processados com sucesso: %d/%d", len(quadros), total_tickers)

    resultado = pd.concat(quadros, ignore_index=True)

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

    logger.info("Total de registros obtidos: %d", len(resultado))
    logger.info(
        "Período coberto: %s até %s",
        resultado["Date"].min(),
        resultado["Date"].max(),
    )
    logger.info("=== BUSCA FINALIZADA ===")

    return resultado


def criar_dataframe_vazio(schema: Any) -> DataFrame:
    """Cria um DataFrame vazio com o schema especificado."""
    return spark.createDataFrame([], schema)


# ====
# FUNÇÕES BACEN - REFATORADAS COM SGS
# ====


def buscar_series_bacen(
    series: Dict[str, int],
    inicio: str,
    fim: str,
) -> pd.DataFrame:
    """
    Busca séries temporais do BACEN usando a biblioteca sgs.
    """
    logger.info("=== INICIANDO BUSCA DE SÉRIES BACEN COM SGS ===")
    logger.info("Séries solicitadas: %s", list(series.keys()))
    logger.info("Período: %s até %s", inicio, fim)

    if sgs is None:
        erro_msg = (
            "Biblioteca sgs não encontrada. "
            "Instale com: pip install sgs"
        )
        logger.error(erro_msg)
        raise ModuleNotFoundError(erro_msg)

    try:
        datetime.strptime(inicio, "%d/%m/%Y")
        datetime.strptime(fim, "%d/%m/%Y")
    except ValueError as e:
        erro_msg = f"Data deve estar no formato DD/MM/YYYY: {str(e)}"
        logger.error(erro_msg)
        raise ValueError(erro_msg) from e

    if not series:
        logger.warning("Nenhuma série fornecida!")
        return pd.DataFrame(columns=["data", "valor", "serie"])

    quadros: List[pd.DataFrame] = []
    erros: List[str] = []

    for nome_serie, codigo_serie in series.items():
        try:
            logger.info(
                "Buscando série '%s' (código: %s)",
                nome_serie,
                codigo_serie,
            )

            serie_temporal = sgs.time_serie(
                codigo_serie,
                start=inicio,
                end=fim,
            )

            if serie_temporal is None or serie_temporal.empty:
                logger.warning(
                    "Nenhum dado retornado para '%s' (código %s)",
                    nome_serie,
                    codigo_serie,
                )
                erros.append(f"Nenhum dado retornado para {nome_serie}")
                continue

            df_serie = serie_temporal.to_frame(name="valor")
            df_serie = df_serie.reset_index()
            df_serie.columns = ["data", "valor"]
            df_serie["serie"] = nome_serie

            df_serie["data"] = pd.to_datetime(df_serie["data"], errors="coerce")
            df_serie["valor"] = pd.to_numeric(df_serie["valor"], errors="coerce")

            antes = len(df_serie)
            df_serie = df_serie.dropna(subset=["data", "valor"])
            depois = len(df_serie)

            if antes > depois:
                logger.warning(
                    "Removidas %d linhas com dados inválidos de '%s'",
                    antes - depois,
                    nome_serie,
                )

            if df_serie.empty:
                logger.warning("Todos os dados de '%s' eram inválidos", nome_serie)
                erros.append(f"Dados inválidos para série {nome_serie}")
                continue

            quadros.append(df_serie)
            logger.info("✓ %s: %d registros obtidos", nome_serie, len(df_serie))

        except Exception as e:
            erro_msg = (
                f"Erro ao buscar '{nome_serie}' (código {codigo_serie}): {str(e)}"
            )
            logger.error(erro_msg)
            erros.append(erro_msg)
            continue

    if erros:
        logger.warning("=== AVISOS DURANTE A BUSCA (%d) ===", len(erros))
        for erro in erros:
            logger.warning("  - %s", erro)

    if not quadros:
        logger.error("Nenhum dado encontrado para nenhuma série!")
        return pd.DataFrame(columns=["data", "valor", "serie"])

    logger.info("=== CONSOLIDANDO RESULTADOS ===")
    resultado_final = pd.concat(quadros, ignore_index=True)

    resultado_final = resultado_final.sort_values(
        ["serie", "data"],
    ).reset_index(drop=True)

    logger.info("✓ Total de registros obtidos: %d", len(resultado_final))
    logger.info(
        "✓ Séries com dados: %d",
        resultado_final["serie"].nunique(),
    )
    logger.info(
        "✓ Período coberto: %s até %s",
        resultado_final["data"].min(),
        resultado_final["data"].max(),
    )
    logger.info("=== BUSCA CONCLUÍDA COM SUCESSO ===")

    return resultado_final


def buscar_multiplas_series_bacen(
    codigos_series: Union[List[int], Dict[str, int]],
    inicio: str,
    fim: str,
) -> pd.DataFrame:
    """
    Busca múltiplas séries do BACEN de forma otimizada usando sgs.dataframe.
    """
    logger.info("=== BUSCA OTIMIZADA DE MÚLTIPLAS SÉRIES BACEN ===")

    if sgs is None:
        raise ModuleNotFoundError(
            "Biblioteca sgs não encontrada. Instale com: pip install sgs"
        )

    try:
        datetime.strptime(inicio, "%d/%m/%Y")
        datetime.strptime(fim, "%d/%m/%Y")
    except ValueError as e:
        raise ValueError(
            f"Data deve estar no formato DD/MM/YYYY: {str(e)}"
        ) from e

    try:
        if isinstance(codigos_series, dict):
            codigos = list(codigos_series.values())
            nomes = list(codigos_series.keys())
            logger.info("Buscando %d séries: %s", len(codigos), nomes)
        else:
            codigos = codigos_series
            nomes = None
            logger.info("Buscando %d séries: %s", len(codigos), codigos)

        df = sgs.dataframe(
            codigos,
            start=inicio,
            end=fim,
        )

        if nomes:
            mapeamento = dict(zip(codigos, nomes))
            df.columns = [mapeamento.get(col, col) for col in df.columns]

        logger.info("✓ %d registros obtidos para %d séries", len(df), len(df.columns))
        logger.info("✓ Período: %s até %s", df.index.min(), df.index.max())

        return df

    except Exception as e:
        logger.error("Erro ao buscar séries: %s", str(e))
        raise


def buscar_indices_futuros(
    indices_config: Dict[str, Dict[str, str]],
    inicio: Optional[str] = None,
    fim: Optional[str] = None,
    range_periodo: str = "3mo",
    interval: str = "1d",
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Busca HISTÓRICO de índices globais via brapi.dev, de forma incremental
    (um índice por vez), replicando a robustez de `buscar_historico_b3`.
    """
    logger.info(
        "=== INICIANDO BUSCA HISTÓRICA DE ÍNDICES GLOBAIS COM BRAPI.DEV ==="
    )

    # Colunas-alvo, alinhadas com a tabela bronze_indices_futuros
    colunas = [
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
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjustedClose",
        "trade_date",
    ]

    if not indices_config:
        logger.warning("Nenhum índice futuro configurado.")
        return pd.DataFrame(columns=colunas)

    # Token da API
    dbutils = DBUtils(spark)
    brapi_token = api_key or dbutils.secrets.get("brapi_scope", "BRAPI_TOKEN")
    base_url = "https://brapi.dev/api/quote"

    # Controle de datas (opcional, mas com mesma lógica do buscar_historico_b3)
    inicio_fmt: Optional[str] = None
    fim_fmt: Optional[str] = None
    range_api = range_periodo  # padrão

    if inicio and fim:
        logger.info("Período solicitado para índices: %s até %s", inicio, fim)

        try:
            inicio_fmt = _converter_data(inicio)
            fim_fmt = _converter_data(fim)
        except ValueError as e:
            logger.error("Erro ao validar datas para índices: %s", str(e))
            raise

        data_inicio = datetime.strptime(inicio, "%d/%m/%Y")
        data_fim = datetime.strptime(fim, "%d/%m/%Y")
        dias_diferenca = (data_fim - data_inicio).days

        # Limite do plano básico (90 dias)
        if dias_diferenca > 90:
            logger.warning(
                "⚠️ ATENÇÃO: Período solicitado (%d dias) excede o limite do "
                "plano básico (90 dias) para índices",
                dias_diferenca,
            )
            logger.warning("Ajustando para buscar apenas os últimos 3 meses")
            data_inicio = data_fim - timedelta(days=90)
            inicio_fmt = data_inicio.strftime("%Y-%m-%d")
            logger.info(
                "Novo período para índices: %s até %s",
                data_inicio.strftime("%d/%m/%Y"),
                fim,
            )

        # Para histórico, usamos 3 meses de qualquer forma (mesmo padrão da B3)
        range_api = "3mo"
    else:
        logger.info(
            "Nenhum período explícito informado para índices. "
            "Usando range='%s' e interval='%s'.",
            range_api,
            interval,
        )

    quadros: List[pd.DataFrame] = []
    erros: List[str] = []
    avisos: List[str] = []

    indices_items = list(indices_config.items())
    total_indices = len(indices_items)

    logger.info("Índices configurados: %d", total_indices)
    logger.info(
        "Lista de índices (nome -> ticker): %s",
        ", ".join(
            f"{nome}={cfg.get('ticker', '')}" for nome, cfg in indices_items
        ),
    )

    delay_entre_requisicoes = 2
    max_tentativas = 3

    for idx, (nome_indice, cfg) in enumerate(indices_items, start=1):
        ticker = (cfg.get("ticker") or "").strip()
        if not ticker:
            aviso = (
                f"Índice '{nome_indice}' sem ticker configurado. Ignorando."
            )
            logger.warning(aviso)
            avisos.append(aviso)
            continue

        symbol = ticker
        logger.info(
            "[%d/%d] Processando índice '%s' (ticker=%s)",
            idx,
            total_indices,
            nome_indice,
            symbol,
        )

        # Delay para mitigar rate limit, igual à busca da B3
        if idx > 1:
            logger.info(
                "Aguardando %ds antes da próxima requisição...",
                delay_entre_requisicoes,
            )
            time.sleep(delay_entre_requisicoes)

        url = f"{base_url}/{symbol}"
        params = {
            "range": range_api,
            "interval": interval,
            "fundamental": "false",
            "token": brapi_token,
        }

        logger.info("Requisição: GET %s", url)
        logger.debug("Parâmetros: %s", params)

        tentativa = 0
        resposta: Optional[requests.Response] = None

        # Loop de tentativas com tratamento explícito de rate limit e HTTPError
        while tentativa < max_tentativas:
            tentativa += 1
            try:
                logger.info(
                    "Tentativa %d/%d para índice %s (%s)",
                    tentativa,
                    max_tentativas,
                    nome_indice,
                    symbol,
                )
                resposta = requests.get(url, params=params, timeout=30)

                # Rate limit (HTTP 429)
                if resposta.status_code == 429:
                    if tentativa < max_tentativas:
                        logger.warning(
                            "Rate limit atingido para %s. Aguardando 10s antes "
                            "de tentar novamente...",
                            symbol,
                        )
                        time.sleep(10)
                        continue

                    erro_msg = (
                        f"Rate limit excedido para {symbol} após "
                        f"{max_tentativas} tentativas"
                    )
                    logger.error(erro_msg)
                    erros.append(erro_msg)
                    resposta = None
                    break

                resposta.raise_for_status()
                break  # Sucesso, sai do loop

            except requests.exceptions.HTTPError as http_err:
                if resposta is not None and resposta.status_code == 404:
                    aviso = (
                        f"Ticker {symbol} não encontrado (404). "
                        f"Índice '{nome_indice}' será ignorado."
                    )
                    logger.warning(aviso)
                    avisos.append(aviso)
                    resposta = None
                    break

                erro_msg = f"HTTPError ao buscar {symbol}: {http_err}"
                logger.error(erro_msg)
                erros.append(erro_msg)
                resposta = None
                break

            except Exception as e:
                erro_msg = f"Erro de conexão ao buscar {symbol}: {e}"
                logger.error(erro_msg, exc_info=True)
                erros.append(erro_msg)
                resposta = None
                break

        if not resposta or resposta.status_code != 200:
            continue

        # Parse JSON e validações básicas
        try:
            dados = resposta.json()
        except json.JSONDecodeError as e:
            erro_msg = f"Resposta JSON inválida para {symbol}: {e}"
            logger.error(erro_msg)
            erros.append(erro_msg)
            continue

        if "results" not in dados or not dados["results"]:
            aviso = f"Nenhum resultado retornado para {symbol}"
            logger.warning(aviso)
            avisos.append(aviso)
            continue

        item = dados["results"][0]

        historico = item.get("historicalDataPrice") or []
        if not historico:
            aviso = (
                "Sem dados históricos (historicalDataPrice) para "
                f"{symbol}"
            )
            logger.warning(aviso)
            avisos.append(aviso)
            continue

        df_hist = pd.DataFrame(historico)
        if df_hist.empty:
            aviso = f"historicalDataPrice vazio para {symbol}"
            logger.warning(aviso)
            avisos.append(aviso)
            continue

        # Garante colunas essenciais de preço/volume
        colunas_hist = [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "adjustedClose",
        ]
        for col in colunas_hist:
            if col not in df_hist.columns:
                df_hist[col] = None

        # trade_date a partir de epoch (segundos)
        df_hist["trade_date"] = pd.to_datetime(
            df_hist["date"],
            unit="s",
            errors="coerce",
        )

        # Se inicio/fim foram informados, filtra pelo período (como em buscar_historico_b3)
        if inicio_fmt and fim_fmt:
            df_hist = df_hist[
                (df_hist["trade_date"] >= inicio_fmt)
                & (df_hist["trade_date"] <= fim_fmt)
            ]

        if df_hist.empty:
            aviso = f"Nenhum dado histórico no período para {symbol}"
            logger.warning(aviso)
            avisos.append(aviso)
            continue

        # Enriquecimento com metadados do índice e taxonomia local
        df_hist["symbol"] = item.get("symbol") or symbol
        df_hist["currency"] = item.get("currency")
        df_hist["shortName"] = item.get("shortName")
        df_hist["longName"] = item.get("longName")
        df_hist["logourl"] = item.get("logourl")

        df_hist["indice"] = nome_indice
        df_hist["regiao"] = cfg.get("regiao")
        df_hist["categoria"] = cfg.get("categoria")

        df_hist["fiftyTwoWeekLow"] = item.get("fiftyTwoWeekLow")
        df_hist["fiftyTwoWeekHigh"] = item.get("fiftyTwoWeekHigh")

        # Conversões explícitas de tipos (numéricos)
        numericas = [
            "open",
            "high",
            "low",
            "close",
            "adjustedClose",
            "volume",
            "fiftyTwoWeekLow",
            "fiftyTwoWeekHigh",
        ]
        for col in numericas:
            df_hist[col] = pd.to_numeric(df_hist[col], errors="coerce")

        # Volume: garante zero para nulos, para evitar problemas de tipagem depois
        df_hist["volume"] = df_hist["volume"].fillna(0)

        # trade_date como date puro (YYYY-MM-DD), alinhado com o schema da Bronze
        df_hist["trade_date"] = df_hist["trade_date"].dt.date

        # Garante que todas as colunas existam e na ordem correta
        for col in colunas:
            if col not in df_hist.columns:
                df_hist[col] = None

        df_hist = df_hist[colunas]

        logger.info(
            "✓ %s (%s): %d registros históricos processados",
            nome_indice,
            symbol,
            len(df_hist),
        )
        quadros.append(df_hist)

    # Resumo e consolidação, espelhando o padrão da função da B3
    if avisos:
        logger.warning(
            "=== AVISOS DURANTE A BUSCA DE ÍNDICES (%d) ===",
            len(avisos),
        )
        for aviso in avisos:
            logger.warning("  - %s", aviso)

    if erros:
        logger.error(
            "=== ERROS DURANTE A BUSCA DE ÍNDICES (%d) ===",
            len(erros),
        )
        for erro in erros:
            logger.error("  - %s", erro)

    if not quadros:
        logger.error("Nenhum dado obtido para índices globais.")
        return pd.DataFrame(columns=colunas)

    resultado = pd.concat(quadros, ignore_index=True)

    # Tipagem final consistente
    resultado["trade_date"] = pd.to_datetime(
        resultado["trade_date"],
        errors="coerce",
    ).dt.date

    numericas_final = [
        "open",
        "high",
        "low",
        "close",
        "adjustedClose",
        "volume",
        "fiftyTwoWeekLow",
        "fiftyTwoWeekHigh",
    ]
    for col in numericas_final:
        resultado[col] = pd.to_numeric(resultado[col], errors="coerce")

    logger.info(
        "Total de registros históricos de índices: %d (%d símbolos)",
        len(resultado),
        resultado["symbol"].nunique(),
    )

    return resultado


__all__ = [
    "buscar_historico_b3",
    "buscar_series_bacen",
    "buscar_multiplas_series_bacen",
    "buscar_indices_futuros",
    "criar_dataframe_vazio",
    "spark",
]