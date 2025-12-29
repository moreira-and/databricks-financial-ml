"""Camada Ouro do pipeline Delta Live Tables."""

from __future__ import annotations

import logging

import dlt
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    obter_nome_tabela,
    obter_metadados_tabela,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_eqt_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_eqt_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_tb_mkt_eqt_perf() -> DataFrame:
    """
    Calcula métricas de performance para ações (equity).
    """
    silver = dlt.read(obter_nome_tabela("prata", "tb_mkt_eqt_day"))
    
    w_30d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-30, 0)
    
    df = (
        silver
        .withColumn("ret_diario", F.log(F.col("close_price")) - F.log(F.lag("close_price", 1).over(
            Window.partitionBy("symbol").orderBy("trade_date")
        )))
        .withColumn("monthly_return", F.exp(F.sum("ret_diario").over(w_30d)) - 1)
        .withColumn("volatility", F.stddev("ret_diario").over(w_30d))
        .withColumn("avg_price", F.avg("close_price").over(w_30d))
        .withColumn("avg_volume", F.avg("volume").over(w_30d))
        .withColumn("last_update", F.max("trade_date").over(Window.partitionBy("symbol")))
        .select(
            "symbol",
            "monthly_return",
            "volatility",
            "avg_price",
            "avg_volume",
            "last_update",
        )
        .dropDuplicates(["symbol", "last_update"])
    )
    
    return df


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_dash"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_dash")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_tb_mkt_idx_dash() -> DataFrame:
    """
    Dashboard simples de indicadores macroeconômicos.
    """
    silver = dlt.read(obter_nome_tabela("prata", "tb_mkt_idx_eco"))
    
    w = Window.partitionBy("idx_type").orderBy(F.col("ref_date").cast("timestamp"))
    
    df = (
        silver
        .withColumn("valor_anterior", F.lag("idx_value").over(w))
        .withColumn(
            "mtd_change",
            (F.col("idx_value") - F.col("valor_anterior")) / F.col("valor_anterior")
        )
        .withColumn("ytd_change", F.lit(None).cast("double"))  # placeholder
        .withColumn("trend_6m", F.lit(None).cast("string"))    # placeholder
        .withColumn("last_update", F.max("ref_date").over(Window.partitionBy("idx_type")))
        .select(
            "idx_type",
            "idx_value",
            "mtd_change",
            "ytd_change",
            "trend_6m",
            "last_update",
        )
        .dropDuplicates(["idx_type", "last_update"])
        .withColumnRenamed("idx_value", "current_value")
    )
    
    return df


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_fut_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_fut_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_tb_mkt_idx_fut_perf() -> DataFrame:
    """
    Calcula métricas de performance para índices globais
    a partir de tb_mkt_idx_fut_day.
    """
    silver = dlt.read(obter_nome_tabela("prata", "tb_mkt_idx_fut_day"))
    
    w_30d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-30, 0)
    w_all = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(Window.unboundedPreceding, 0)
    
    df = (
        silver
        .withColumn(
            "ret_diario",
            F.log(F.col("close_price")) - F.log(F.lag("close_price", 1).over(
                Window.partitionBy("symbol").orderBy("trade_date")
            ))
        )
        .withColumn("monthly_return", F.exp(F.sum("ret_diario").over(w_30d)) - 1)
        .withColumn("ytd_return", F.lit(None).cast("double"))  # placeholder
        .withColumn("volatility", F.stddev("ret_diario").over(w_30d))
        .withColumn("avg_price_30d", F.avg("close_price").over(w_30d))
        .withColumn("last_close", F.last("close_price").over(w_all))
        .withColumn("last_update", F.max("trade_date").over(Window.partitionBy("symbol")))
        .withColumn("price_52w_high", F.last("price_52w_high", ignorenulls=True).over(w_all))
        .withColumn("price_52w_low", F.last("price_52w_low", ignorenulls=True).over(w_all))
        .withColumn(
            "pct_from_52w_high",
            (F.col("last_close") - F.col("price_52w_high")) / F.col("price_52w_high")
        )
        .withColumn(
            "pct_from_52w_low",
            (F.col("last_close") - F.col("price_52w_low")) / F.col("price_52w_low")
        )
        .select(
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
        )
        .dropDuplicates(["symbol", "last_update"])
    )
    
    return df
