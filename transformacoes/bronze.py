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
    
    tickers = obter_lista_configuracoes(
        "b3.tickers",
        "PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3,ELET3,B3SA3",
    )
    
    hoje = datetime.utcnow()
    data_final = hoje.strftime("%d/%m/%Y")
    data_inicial = (hoje - pd.DateOffset(months=3)).strftime("%d/%m/%Y")
    
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
        
        pdf = buscar_historico_b3(tickers, data_inicial, data_final)
        
        if pdf.empty:
            logger.warning(
                f"Nenhum dado encontrado para os tickers: {tickers}"
            )
            return criar_dataframe_vazio(schema)
        
        colunas_esperadas = set(schema.fieldNames()) - {"ingestion_timestamp"}
        colunas_recebidas = set(pdf.columns)
        if not colunas_esperadas.issubset(colunas_recebidas):
            faltantes = colunas_esperadas - colunas_recebidas
            raise ValueError(f"Colunas ausentes nos dados: {faltantes}")
        
        ts_ingestao = timestamp_ingestao()
        pdf["ingestion_timestamp"] = ts_ingestao
        logger.info(f"Timestamp de ingestão: {ts_ingestao}")
        
        pdf = pdf.rename(columns={"Stock Splits": "Stock_Splits"})
        
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
        
        df = spark.createDataFrame(pdf, schema=schema)
        logger.info(f"Total de registros coletados: {df.count()}")
        
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
    
    series_padrao = {
        "selic": 1178,
        "cdi": 12,
        "ipca": 433,
        "poupanca": 195,
        "igpm": 189,
        "inpc": 188,
        "igpdi": 190,
        "selic_meta": 432,
    }
    
    series = json.loads(
        obter_configuracao(
            "bacen.series",
            json.dumps(series_padrao),
        )
    )
    
    data_inicial = obter_configuracao("bacen.start_date", "01/01/2020")
    data_final = obter_configuracao(
        "bacen.end_date",
        datetime.utcnow().strftime("%d/%m/%Y"),
    )
    
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
        
        pdf = buscar_series_bacen(series, data_inicial, data_final)
        
        if pdf.empty:
            logger.warning("Nenhum dado retornado do BACEN")
            return criar_dataframe_vazio(schema)
        
        pdf = pdf.dropna(subset=["data"])
        
        pdf["valor"] = pdf["valor"].fillna(0.0)
        pdf["serie"] = pdf["serie"].fillna("NA")
        
        pdf["data"] = pd.to_datetime(pdf["data"], errors="coerce")
        
        ts_ingestao = timestamp_ingestao()
        pdf["ingestion_timestamp"] = ts_ingestao
        logger.info(f"Timestamp de ingestão: {ts_ingestao}")
        
        df = spark.createDataFrame(pdf, schema=schema)
        logger.info(f"Total de registros coletados: {df.count()}")
        
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
    Coleta HISTÓRICO diário de índices globais via brapi.dev.
    
    Expande o campo `historicalDataPrice` da API e gera:
    - 1 linha por índice por dia (histórico de ~3 meses, conforme range)
    - trade_date (Date)
    - ingestion_timestamp
    """
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
    
    schema = T.StructType(
        [
            T.StructField("symbol", T.StringType(), False),
            T.StructField("currency", T.StringType(), True),
            T.StructField("shortName", T.StringType(), True),
            T.StructField("longName", T.StringType(), True),
            T.StructField("logourl", T.StringType(), True),
            T.StructField("indice", T.StringType(), False),
            T.StructField("regiao", T.StringType(), True),
            T.StructField("categoria", T.StringType(), True),
            T.StructField("fiftyTwoWeekLow", T.DoubleType(), True),
            T.StructField("fiftyTwoWeekHigh", T.DoubleType(), True),
            T.StructField("date", T.LongType(), True),   # timestamp unix em segundos
            T.StructField("open", T.DoubleType(), True),
            T.StructField("high", T.DoubleType(), True),
            T.StructField("low", T.DoubleType(), True),
            T.StructField("close", T.DoubleType(), True),
            T.StructField("volume", T.LongType(), True),
            T.StructField("adjustedClose", T.DoubleType(), True),
            T.StructField("trade_date", T.DateType(), True),
            T.StructField("ingestion_timestamp", T.TimestampType(), False),
        ]
    )
    
    try:
        logger.info(
            f"Iniciando captura HISTÓRICA de índices globais: "
            f"{list(indices_cfg.keys())}"
        )
        pdf = buscar_indices_futuros(indices_cfg, range_periodo="3mo", interval="1d")
        
        if pdf.empty:
            logger.warning("Nenhum dado retornado para índices globais.")
            return criar_dataframe_vazio(schema)
        
        colunas_esperadas = set(
            [
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
        )
        colunas_recebidas = set(pdf.columns)
        if not colunas_esperadas.issubset(colunas_recebidas):
            faltantes = colunas_esperadas - colunas_recebidas
            raise ValueError(f"Colunas ausentes nos dados de índices: {faltantes}")
        
        # timestamp de ingestão
        ts_ingestao = timestamp_ingestao()
        pdf["ingestion_timestamp"] = ts_ingestao
        logger.info(f"Timestamp de ingestão (índices globais): {ts_ingestao}")
        
        # Garante tipos
        pdf["trade_date"] = pd.to_datetime(pdf["trade_date"], errors="coerce").dt.date
        
        numericas = [
            "open",
            "high",
            "low",
            "close",
            "adjustedClose",
            "fiftyTwoWeekLow",
            "fiftyTwoWeekHigh",
        ]
        for col in numericas:
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")
        
        if "volume" in pdf.columns:
            pdf["volume"] = pd.to_numeric(pdf["volume"], errors="coerce").fillna(0).astype("int64")
        
        pdf = pdf[[field.name for field in schema.fields]]
        df = spark.createDataFrame(pdf, schema=schema)
        logger.info(f"Total de registros de índices globais (histórico): {df.count()}")
        
        return df.repartition("symbol").sortWithinPartitions("trade_date")
    
    except Exception as e:
        logger.error(f"Erro ao processar índices globais: {str(e)}")
        logger.error("Stack trace completo:", exc_info=True)
        return criar_dataframe_vazio(schema)


__all__ = [
    "bronze_cotacoes_b3",
    "bronze_series_bacen",
    "bronze_indices_futuros",
]
