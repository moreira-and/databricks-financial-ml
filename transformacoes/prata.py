"""Camada Prata do pipeline Delta Live Tables."""

from __future__ import annotations

import logging

import dlt
from pyspark.sql import DataFrame, functions as F, types as T

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    obter_nome_tabela,
    obter_metadados_tabela,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_eqt_day"),
    comment=obter_metadados_tabela("prata", "tb_mkt_eqt_day")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"],
)
def prata_tb_mkt_eqt_day() -> DataFrame:
    """
    Normaliza a série histórica diária de ações (equity) a partir da bronze.cotacoes_b3.
    """
    bronze = dlt.read(obter_nome_tabela("bronze", "cotacoes_b3"))
    
    df = (
        bronze
        .withColumn("trade_date", F.to_date("Date"))
        .withColumnRenamed("Open", "open_price")
        .withColumnRenamed("High", "high_price")
        .withColumnRenamed("Low", "low_price")
        .withColumnRenamed("Close", "close_price")
        .withColumnRenamed("Volume", "volume")
        .withColumn("div_cash", F.col("Dividends").cast(T.DoubleType()))
        .withColumn("split_ratio", F.col("Stock_Splits").cast(T.DoubleType()))
        .select(
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "div_cash",
            "split_ratio",
            "ticker",
        )
        .withColumnRenamed("ticker", "symbol")
    )
    
    return df


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_idx_eco"),
    comment=obter_metadados_tabela("prata", "tb_mkt_idx_eco")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"],
)
def prata_tb_mkt_idx_eco() -> DataFrame:
    """
    Normaliza indicadores econômicos do BACEN a partir da bronze.series_bacen.
    """
    bronze = dlt.read(obter_nome_tabela("bronze", "series_bacen"))
    
    df = (
        bronze
        .withColumn("ref_date", F.to_date("data"))
        .withColumnRenamed("valor", "idx_value")
        .withColumnRenamed("serie", "idx_type")
        .withColumn("frequency", F.lit("unknown"))
        .select(
            "ref_date",
            "idx_value",
            "idx_type",
            "frequency",
        )
    )
    
    return df


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_idx_fut_day"),
    comment=obter_metadados_tabela("prata", "tb_mkt_idx_fut_day")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"],
)
def prata_tb_mkt_idx_fut_day() -> DataFrame:
    """
    Normaliza a série histórica diária de índices globais a partir da bronze.indices_futuros.
    
    Entradas (bronze.indices_futuros):
    - symbol, currency, shortName, longName, logourl, indice, regiao, categoria
    - fiftyTwoWeekLow, fiftyTwoWeekHigh
    - date (unix ts), open, high, low, close, volume, adjustedClose
    - trade_date (date)
    
    Saída (tb_mkt_idx_fut_day):
    - trade_datetime (a partir de date)
    - trade_date
    - open_price, high_price, low_price, close_price
    - prev_close_price (lag por symbol)
    - abs_change, pct_change
    - volume
    - index_symbol, index_name, region, category, currency
    - price_52w_high, price_52w_low
    """
    bronze = dlt.read(obter_nome_tabela("bronze", "indices_futuros"))
    
    df = (
        bronze
        # converte unix timestamp em datetime
        .withColumn("trade_datetime", F.to_timestamp(F.col("date")))
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumnRenamed("open", "open_price")
        .withColumnRenamed("high", "high_price")
        .withColumnRenamed("low", "low_price")
        .withColumnRenamed("close", "close_price")
        .withColumnRenamed("volume", "volume")
        .withColumnRenamed("symbol", "index_symbol")
        .withColumnRenamed("shortName", "index_name")
        .withColumnRenamed("regiao", "region")
        .withColumnRenamed("categoria", "category")
        .withColumnRenamed("fiftyTwoWeekHigh", "price_52w_high")
        .withColumnRenamed("fiftyTwoWeekLow", "price_52w_low")
    )
    
    
    from pyspark.sql.window import Window
    w = Window.partitionBy("index_symbol").orderBy("trade_date")
    
    df = (
        df
        .withColumn("prev_close_price", F.lag("close_price").over(w))
        .withColumn(
            "abs_change",
            F.when(F.col("prev_close_price").isNull(), F.lit(0.0))
            .otherwise(F.col("close_price") - F.col("prev_close_price"))
        )
        .withColumn(
            "pct_change",
            F.when(
                (F.col("prev_close_price").isNull()) | (F.col("prev_close_price") == 0),
                F.lit(0.0)
            ).otherwise(F.col("abs_change") / F.col("prev_close_price"))
        )
        .select(
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
            "index_symbol",
            "index_name",
            "region",
            "category",
            "currency",
            "price_52w_high",
            "price_52w_low",
        )
        .withColumnRenamed("index_symbol", "symbol")
    )
    
    return df
