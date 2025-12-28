"""Camada Bronze do pipeline Delta Live Tables."""

from __future__ import annotations

import json
import logging
from datetime import datetime

import dlt
import pandas as pd
from pyspark.sql import DataFrame, types as T

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    INDICES_FUTUROS_PADRAO,
    obter_configuracao,
    obter_lista_configuracoes,
    obter_nome_tabela,
    obter_metadados_tabela,
    timestamp_ingestao,
)
from utilitarios.fontes_dados import (
    buscar_historico_b3,
    buscar_series_bacen,
    buscar_indices_futuros,
    criar_dataframe_vazio,
    spark,
)

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dlt.table(
    name=obter_nome_tabela("bronze", "cotacoes_b3"),
    comment=obter_metadados_tabela("bronze", "cotacoes_b3")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["bronze"],
)
def bronze_cotacoes_b3() -> DataFrame:
    """Coleta cotações históricas da B3 na API do Yahoo Finance."""
    
    # Configurações com valores padrão
    tickers = obter_lista_configuracoes(
        "b3.tickers",
        "PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3,ELET3,B3SA3",
    )
    
    # Datas no formato correto (DD/MM/YYYY)
    hoje = datetime.utcnow()
    data_final = hoje.strftime("%d/%m/%Y")
    data_inicial = (hoje - pd.DateOffset(months=3)).strftime("%d/%m/%Y")
    
    # Schema esperado para validação
    schema = T.StructType(
        [
            T.StructField("Date", T.TimestampType(), False),
            T.StructField("Open", T.DoubleType(), True),
            T.StructField("High", T.DoubleType(), True),
            T.StructField("Low", T.DoubleType(), True),
            T.StructField("Close", T.DoubleType(), True),
            T.StructField("Volume", T.DoubleType(), True),
            T.StructField("Dividends", T.DoubleType(), True),
            T.StructField("Stock_Splits", T.DoubleType(), True),
            T.StructField("ticker", T.StringType(), False),
            T.StructField("ingestion_timestamp", T.TimestampType(), False),
        ]
    )
    
    try:
        logger.info(
            f"Iniciando coleta de dados B3 para {len(tickers)} tickers"
        )
        logger.info(f"Período: {data_inicial} até {data_final}")
        
        # Busca dados da API
        pdf = buscar_historico_b3(tickers, data_inicial, data_final)
        
        if pdf.empty:
            logger.warning(
                f"Nenhum dado encontrado para os tickers: {tickers}"
            )
            return criar_dataframe_vazio(schema)
        
        # Validação básica dos dados recebidos
        colunas_esperadas = set(schema.fieldNames()) - {"ingestion_timestamp"}
        colunas_recebidas = set(pdf.columns)
        if not colunas_esperadas.issubset(colunas_recebidas):
            faltantes = colunas_esperadas - colunas_recebidas
            raise ValueError(f"Colunas ausentes nos dados: {faltantes}")
        
        # Adiciona timestamp de ingestão
        ts_ingestao = timestamp_ingestao()
        pdf["ingestion_timestamp"] = ts_ingestao
        logger.info(f"Timestamp de ingestão: {ts_ingestao}")
        
        # Garante tipos corretos e nomes de colunas padronizados
        pdf = pdf.rename(columns={"Stock Splits": "Stock_Splits"})
        
        # Trata valores nulos nas datas antes de converter
        pdf["Date"] = pd.to_datetime(pdf["Date"], errors="coerce")
        
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
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype(float)
        
        # Converte para DataFrame Spark com schema validado
        df = spark.createDataFrame(pdf, schema=schema)
        logger.info(f"Total de registros coletados: {df.count()}")
        
        # Otimiza particionamento e ordenação
        return df.repartition("ticker").sortWithinPartitions("Date")
    
    except ValueError as e:
        logger.error(f"Erro de validação nos dados B3: {str(e)}")
        return criar_dataframe_vazio(schema)
    except Exception as e:
        logger.error(f"Erro inesperado ao processar cotações B3: {str(e)}")
        logger.error("Stack trace completo:", exc_info=True)
        return criar_dataframe_vazio(schema)


@dlt.table(
    name=obter_nome_tabela("bronze", "series_bacen"),
    comment=obter_metadados_tabela("bronze", "series_bacen")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["bronze"],
)
def bronze_series_bacen() -> DataFrame:
    """Busca séries temporais no serviço SGS do BACEN."""
    
    # Séries econômicas a serem capturadas
    series_padrao = {
        "selic": 1178,       # Taxa Selic diária
        "cdi": 12,           # CDI diário
        "ipca": 433,         # IPCA mensal
        "poupanca": 195,     # Rendimento poupança mensal
        "igpm": 189,         # IGP-M mensal
        "inpc": 188,         # INPC mensal
        "igpdi": 190,        # IGP-DI mensal
        "selic_meta": 432,   # Meta Selic definida pelo COPOM
    }
    
    # Obtém configuração ou usa padrão
    series = json.loads(
        obter_configuracao(
            "bacen.series",
            json.dumps(series_padrao),
        )
    )
    
    # Datas no formato correto (DD/MM/YYYY)
    data_inicial = obter_configuracao("bacen.start_date", "01/01/2020")
    data_final = obter_configuracao(
        "bacen.end_date",
        datetime.utcnow().strftime("%d/%m/%Y"),
    )
    
    # Schema esperado
    schema = T.StructType(
        [
            T.StructField("data", T.TimestampType(), False),
            T.StructField("valor", T.DoubleType(), False),
            T.StructField("serie", T.StringType(), False),
            T.StructField("ingestion_timestamp", T.TimestampType(), False),
        ]
    )
    
    try:
        logger.info(f"Iniciando coleta de {len(series)} séries do BACEN")
        logger.info(f"Período: {data_inicial} até {data_final}")
        
        # Busca dados da API
        pdf = buscar_series_bacen(series, data_inicial, data_final)
        
        if pdf.empty:
            logger.warning("Nenhum dado retornado do BACEN")
            return criar_dataframe_vazio(schema)
        
        # Remove linhas com data nula antes de preencher
        pdf = pdf.dropna(subset=["data"])
        
        # Preenche valores nulos nas colunas numéricas e de texto
        pdf["valor"] = pdf["valor"].fillna(0.0)
        pdf["serie"] = pdf["serie"].fillna("NA")
        
        # Trata valores nulos nas datas
        pdf["data"] = pd.to_datetime(pdf["data"], errors="coerce")
        
        # Adiciona timestamp de ingestão
        ts_ingestao = timestamp_ingestao()
        pdf["ingestion_timestamp"] = ts_ingestao
        logger.info(f"Timestamp de ingestão: {ts_ingestao}")
        
        # Converte para DataFrame Spark com schema validado
        df = spark.createDataFrame(pdf, schema=schema)
        logger.info(f"Total de registros coletados: {df.count()}")
        
        # Otimiza particionamento
        return df.repartition("serie")
    
    except Exception as e:
        logger.error(f"Erro ao processar séries BACEN: {str(e)}")
        logger.error("Stack trace completo:", exc_info=True)
        return criar_dataframe_vazio(schema)


@dlt.table(
    name=obter_nome_tabela("bronze", "indices_futuros"),
    comment=obter_metadados_tabela("bronze", "indices_futuros")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["bronze"],
)
def bronze_indices_futuros() -> DataFrame:
    """
    Coleta cotações atuais de derivativos de índices globais via brapi.dev.
    
    Mantém o schema original da API (regularMarket*) acrescido de:
    - indice: identificador lógico (ex: IndSp500, IndBovespa)
    - regiao: macro região
    - categoria: tipo de índice
    """
    # Permite override via spark.conf / env, mas mantém padrão do repositório
    indices_cfg_str = obter_configuracao(
        "indices.futuros.config",
        json.dumps(INDICES_FUTUROS_PADRAO),
    )
    try:
        indices_cfg = json.loads(indices_cfg_str)
    except json.JSONDecodeError as e:
        logger.warning(
            f"Configuração de índices futuros inválida ({e}), "
            "recuando para configuração padrão."
        )
        indices_cfg = INDICES_FUTUROS_PADRAO
    
    # Schema esperado
    schema = T.StructType(
        [
            T.StructField("currency", T.StringType(), True),
            T.StructField("shortName", T.StringType(), True),
            T.StructField("longName", T.StringType(), True),
            T.StructField("regularMarketChange", T.DoubleType(), True),
            T.StructField("regularMarketChangePercent", T.DoubleType(), True),
            T.StructField("regularMarketTime", T.TimestampType(), True),
            T.StructField("regularMarketPrice", T.DoubleType(), True),
            T.StructField("regularMarketDayHigh", T.DoubleType(), True),
            T.StructField("regularMarketDayLow", T.DoubleType(), True),
            T.StructField("regularMarketDayRange", T.StringType(), True),
            T.StructField("regularMarketPreviousClose", T.DoubleType(), True),
            T.StructField("regularMarketOpen", T.DoubleType(), True),
            T.StructField("regularMarketVolume", T.DoubleType(), True),
            T.StructField("fiftyTwoWeekRange", T.StringType(), True),
            T.StructField("fiftyTwoWeekLow", T.DoubleType(), True),
            T.StructField("fiftyTwoWeekHigh", T.DoubleType(), True),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("logourl", T.StringType(), True),
            T.StructField("indice", T.StringType(), False),
            T.StructField("regiao", T.StringType(), True),
            T.StructField("categoria", T.StringType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), False),
        ]
    )
    
    try:
        logger.info(
            f"Iniciando captura de derivativos de índices: "
            f"{list(indices_cfg.keys())}"
        )
        pdf = buscar_indices_futuros(indices_cfg)
        
        if pdf.empty:
            logger.warning("Nenhum dado retornado para índices futuros.")
            return criar_dataframe_vazio(schema)
        
        colunas_esperadas = set(
            [
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
        )
        colunas_recebidas = set(pdf.columns)
        if not colunas_esperadas.issubset(colunas_recebidas):
            faltantes = colunas_esperadas - colunas_recebidas
            raise ValueError(f"Colunas ausentes nos dados de índices: {faltantes}")
        
        # Conversão de tipos básicos
        pdf["regularMarketTime"] = pd.to_datetime(
            pdf["regularMarketTime"], errors="coerce"
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
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")
        
        # Timestamp de ingestão
        ts_ingestao = timestamp_ingestao()
        pdf["ingestion_timestamp"] = ts_ingestao
        logger.info(f"Timestamp de ingestão (índices futuros): {ts_ingestao}")
        
        # Cria DataFrame Spark com schema explícito
        pdf = pdf[[field.name for field in schema.fields]]
        df = spark.createDataFrame(pdf, schema=schema)
        logger.info(f"Total de registros de índices futuros: {df.count()}")
        
        return df.repartition("indice").sortWithinPartitions("regularMarketTime")
    
    except Exception as e:
        logger.error(f"Erro ao processar índices futuros: {str(e)}")
        logger.error("Stack trace completo:", exc_info=True)
        return criar_dataframe_vazio(schema)


__all__ = [
    "bronze_cotacoes_b3",
    "bronze_series_bacen",
    "bronze_indices_futuros",
]
