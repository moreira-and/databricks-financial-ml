"""Camada Ouro do pipeline Delta Live Tables."""

from __future__ import annotations

import dlt
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    obter_nome_tabela,
    obter_metadados_tabela,
)

NOME_PRATA_EQT = obter_nome_tabela("prata", "tb_mkt_eqt_day")
NOME_PRATA_BACEN = obter_nome_tabela("prata", "tb_mkt_idx_eco")
NOME_PRATA_IDX_FUT = obter_nome_tabela("prata", "tb_mkt_idx_fut_day")


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_eqt_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_eqt_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_metricas_b3() -> DataFrame:
    """
    Consolida métricas de performance de ativos de equity.
    
    Deriva, por símbolo:
    - monthly_return: retorno percentual aproximado dos últimos ~21 pregões
    - volatility: desvio padrão dos retornos diários (últimos ~21 pregões)
    - avg_price: preço médio de fechamento (últimos ~21 pregões)
    - avg_volume: volume médio negociado (últimos ~21 pregões)
    - last_update: última data de negociação disponível
    """
    df = dlt.read(NOME_PRATA_EQT)
    
    df = df.withColumn("trade_date", F.to_date("trade_date"))
    
    w_symbol_asc = Window.partitionBy("symbol").orderBy("trade_date")
    w_symbol_desc = Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())
    w_last_21 = (
        Window.partitionBy("symbol")
        .orderBy(F.col("trade_date").desc())
        .rowsBetween(0, 20)
    )
    
    df_enriched = (
        df.withColumn(
            "daily_return",
            (
                F.col("close_price")
                / F.lag("close_price").over(w_symbol_asc)
                - 1.0
            ),
        )
        .withColumn(
            "close_21d_ago",
            F.lag("close_price", 21).over(w_symbol_asc),
        )
    )
    
    df_metrics = (
        df_enriched.withColumn(
            "monthly_return",
            F.when(
                F.col("close_21d_ago").isNotNull(),
                (F.col("close_price") / F.col("close_21d_ago") - 1.0) * 100,
            ),
        )
        .withColumn(
            "volatility",
            F.stddev("daily_return").over(w_last_21) * 100,
        )
        .withColumn(
            "avg_price",
            F.avg("close_price").over(w_last_21),
        )
        .withColumn(
            "avg_volume",
            F.avg("volume").over(w_last_21),
        )
        .withColumn(
            "row_num",
            F.row_number().over(w_symbol_desc),
        )
    )
    
    df_latest = df_metrics.filter(F.col("row_num") == 1)
    
    return df_latest.select(
        F.col("symbol"),
        F.round(F.col("monthly_return"), 4).alias("monthly_return"),
        F.round(F.col("volatility"), 4).alias("volatility"),
        F.round(F.col("avg_price"), 4).alias("avg_price"),
        F.round(F.col("avg_volume"), 0).alias("avg_volume"),
        F.col("trade_date").alias("last_update"),
    )


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_dash"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_dash")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_indicadores_bacen() -> DataFrame:
    """
    Consolida indicadores macroeconômicos do BACEN para consumo em dashboards.
    
    Para cada idx_type (ex: selic, ipca):
    - current_value: último valor disponível
    - mtd_change: variação percentual no mês corrente
    - ytd_change: variação percentual no ano corrente
    - trend_6m: tendência dos últimos 6 meses (up/down/flat)
    - last_update: última data de observação
    """
    df = dlt.read(NOME_PRATA_BACEN)
    df = df.withColumn("ref_date", F.to_date("ref_date"))
    
    current_date = F.current_date()
    current_year = F.year(current_date)
    current_month = F.month(current_date)
    date_6m_ago = F.add_months(current_date, -6)
    
    w_type_desc = Window.partitionBy("idx_type").orderBy(F.col("ref_date").desc())
    
    # Último valor disponível
    df_latest = (
        df.withColumn("row_num", F.row_number().over(w_type_desc))
        .filter(F.col("row_num") == 1)
        .select(
            "idx_type",
            F.col("idx_value").alias("current_value"),
            F.col("ref_date").alias("last_update"),
        )
    )
    
    # Primeiro valor do ano corrente
    df_year = df.withColumn("year", F.year("ref_date"))
    w_year = Window.partitionBy("idx_type", "year").orderBy("ref_date")
    df_year_first = (
        df_year.withColumn("row_num_year", F.row_number().over(w_year))
        .filter((F.col("row_num_year") == 1) & (F.col("year") == current_year))
        .select(
            "idx_type",
            F.col("idx_value").alias("value_year_start"),
        )
    )
    
    # Primeiro valor do mês corrente
    df_month = df_year.withColumn("month", F.month("ref_date"))
    w_month = Window.partitionBy("idx_type", "year", "month").orderBy("ref_date")
    df_month_first = (
        df_month.withColumn("row_num_month", F.row_number().over(w_month))
        .filter(
            (F.col("row_num_month") == 1)
            & (F.col("year") == current_year)
            & (F.col("month") == current_month)
        )
        .select(
            "idx_type",
            F.col("idx_value").alias("value_month_start"),
        )
    )
    
    # Valor há ~6 meses (última observação em ref_date <= date_6m_ago)
    df_6m = df.filter(F.col("ref_date") <= date_6m_ago)
    w_6m_desc = Window.partitionBy("idx_type").orderBy(F.col("ref_date").desc())
    df_6m_last = (
        df_6m.withColumn("row_num_6m", F.row_number().over(w_6m_desc))
        .filter(F.col("row_num_6m") == 1)
        .select(
            "idx_type",
            F.col("idx_value").alias("value_6m_ago"),
        )
    )
    
    df_dash = (
        df_latest.join(df_year_first, on="idx_type", how="left")
        .join(df_month_first, on="idx_type", how="left")
        .join(df_6m_last, on="idx_type", how="left")
    )
    
    df_dash = (
        df_dash.withColumn(
            "ytd_change",
            F.when(
                F.col("value_year_start").isNotNull()
                & (F.col("value_year_start") != 0),
                (F.col("current_value") / F.col("value_year_start") - 1.0) * 100,
            ),
        )
        .withColumn(
            "mtd_change",
            F.when(
                F.col("value_month_start").isNotNull()
                & (F.col("value_month_start") != 0),
                (F.col("current_value") / F.col("value_month_start") - 1.0) * 100,
            ),
        )
        .withColumn(
            "trend_6m",
            F.when(
                F.col("value_6m_ago").isNull(),
                F.lit("NA"),
            )
            .when(
                F.col("value_6m_ago") == 0,
                F.lit("NA"),
            )
            .when(
                (F.col("current_value") / F.col("value_6m_ago") - 1.0) * 100 > 2.0,
                F.lit("up"),
            )
            .when(
                (F.col("current_value") / F.col("value_6m_ago") - 1.0) * 100 < -2.0,
                F.lit("down"),
            )
            .otherwise(F.lit("flat")),
        )
    )
    
    return df_dash.select(
        "idx_type",
        F.round("current_value", 4).alias("current_value"),
        F.round("mtd_change", 4).alias("mtd_change"),
        F.round("ytd_change", 4).alias("ytd_change"),
        "trend_6m",
        "last_update",
    )


@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_fut_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_fut_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_indices_futuros() -> DataFrame:
    """
    Consolida métricas de performance de derivativos de índices globais.
    
    Fonte: tb_mkt_idx_fut_day (camada prata).
    
    Métricas por symbol (ex: IndSp500, IndBovespa):
    - monthly_return: retorno percentual dos últimos ~21 pregões
    - ytd_return: retorno aproximado em ~252 pregões
    - volatility: desvio padrão dos retornos diários (últimos ~21 pregões)
    - avg_price_30d: preço médio de fechamento nos últimos ~21 pregões
    - last_close: último preço de fechamento
    - price_52w_high, price_52w_low: máximas/mínimas de 52 semanas
    - pct_from_52w_high / pct_from_52w_low: distância percentual das extremas
    - last_update: última data de negociação
    """
    df = dlt.read(NOME_PRATA_IDX_FUT)
    df = df.withColumn("trade_date", F.to_date("trade_date"))
    
    w_symbol_asc = Window.partitionBy("symbol").orderBy("trade_date")
    w_symbol_desc = Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())
    w_last_21 = (
        Window.partitionBy("symbol")
        .orderBy(F.col("trade_date").desc())
        .rowsBetween(0, 20)
    )
    
    df_enriched = (
        df.withColumn(
            "daily_return",
            (
                F.col("close_price")
                / F.lag("close_price").over(w_symbol_asc)
                - 1.0
            ),
        )
        .withColumn(
            "close_21d_ago",
            F.lag("close_price", 21).over(w_symbol_asc),
        )
        .withColumn(
            "close_252d_ago",
            F.lag("close_price", 252).over(w_symbol_asc),
        )
    )
    
    df_metrics = (
        df_enriched.withColumn(
            "monthly_return",
            F.when(
                F.col("close_21d_ago").isNotNull(),
                (F.col("close_price") / F.col("close_21d_ago") - 1.0) * 100,
            ),
        )
        .withColumn(
            "ytd_return",
            F.when(
                F.col("close_252d_ago").isNotNull(),
                (F.col("close_price") / F.col("close_252d_ago") - 1.0) * 100,
            ),
        )
        .withColumn(
            "volatility",
            F.stddev("daily_return").over(w_last_21) * 100,
        )
        .withColumn(
            "avg_price_30d",
            F.avg("close_price").over(w_last_21),
        )
        .withColumn(
            "pct_from_52w_high",
            F.when(
                F.col("price_52w_high").isNotNull()
                & (F.col("price_52w_high") != 0),
                (F.col("close_price") / F.col("price_52w_high") - 1.0) * 100,
            ),
        )
        .withColumn(
            "pct_from_52w_low",
            F.when(
                F.col("price_52w_low").isNotNull()
                & (F.col("price_52w_low") != 0),
                (F.col("close_price") / F.col("price_52w_low") - 1.0) * 100,
            ),
        )
        .withColumn(
            "row_num",
            F.row_number().over(w_symbol_desc),
        )
    )
    
    df_latest = df_metrics.filter(F.col("row_num") == 1)
    
    return df_latest.select(
        "symbol",
        "index_name",
        "region",
        "category",
        "currency",
        F.round("monthly_return", 4).alias("monthly_return"),
        F.round("ytd_return", 4).alias("ytd_return"),
        F.round("volatility", 4).alias("volatility"),
        F.round("avg_price_30d", 4).alias("avg_price_30d"),
        F.col("close_price").alias("last_close"),
        "price_52w_high",
        "price_52w_low",
        F.round("pct_from_52w_high", 4).alias("pct_from_52w_high"),
        F.round("pct_from_52w_low", 4).alias("pct_from_52w_low"),
        F.col("trade_date").alias("last_update"),
    )


__all__ = [
    "ouro_metricas_b3",
    "ouro_indicadores_bacen",
    "ouro_indices_futuros",
]
