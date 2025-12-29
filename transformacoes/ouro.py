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


# ============================================================================
# OURO - tb_mkt_eqt_perf
# ============================================================================

@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_eqt_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_eqt_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_tb_mkt_eqt_perf() -> DataFrame:
    """
    Calcula métricas de performance para ações (equity).

    - monthly_return: retorno acumulado nos últimos ~30 pregões
    - volatility: desvio padrão dos retornos diários nesse período
    """
    silver = dlt.read(obter_nome_tabela("prata", "tb_mkt_eqt_day"))

    # Garantir tipos básicos
    silver = (
        silver
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumn("close_price", F.col("close_price").cast("double"))
    )

    # Janelas
    w_ret = Window.partitionBy("symbol").orderBy("trade_date")
    w_30d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-29, 0)
    w_last = Window.partitionBy("symbol")

    # Retorno diário logarítmico
    silver = silver.withColumn(
        "ret_diario",
        F.log("close_price") - F.log(F.lag("close_price", 1).over(w_ret))
    )

    # Contagem na janela
    silver = silver.withColumn(
        "n_obs_30d",
        F.count("ret_diario").over(w_30d)
    )

    # Última data por símbolo
    silver = silver.withColumn(
        "last_update",
        F.max("trade_date").over(w_last)
    )

    df = (
        silver
        .withColumn(
            "monthly_return",
            F.when(
                F.col("n_obs_30d") >= 2,
                F.exp(F.sum("ret_diario").over(w_30d)) - F.lit(1.0)
            )
        )
        .withColumn(
            "volatility",
            F.when(
                F.col("n_obs_30d") >= 2,
                F.stddev("ret_diario").over(w_30d)
            )
        )
        .withColumn("avg_price", F.avg("close_price").over(w_30d))
        .withColumn("avg_volume", F.avg("volume").over(w_30d))
        # Mantém só o snapshot mais recente de cada símbolo
        .filter(F.col("trade_date") == F.col("last_update"))
        .select(
            "symbol",
            "monthly_return",
            "volatility",
            "avg_price",
            "avg_volume",
            "last_update",
        )
        .dropDuplicates(["symbol"])
    )

    return df


# ============================================================================
# OURO - tb_mkt_idx_dash
# ============================================================================

@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_dash"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_dash")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_tb_mkt_idx_dash() -> DataFrame:
    """
    Dashboard simples de indicadores macroeconômicos.

    - current_value: último valor disponível por idx_type
    - mtd_change: variação % vs último valor do mês anterior
    - ytd_change: variação % vs último valor do ano anterior
    """
    silver = dlt.read(obter_nome_tabela("prata", "tb_mkt_idx_eco"))

    silver = (
        silver
        .withColumn("ref_date", F.to_date("ref_date"))
        .withColumn("idx_value", F.col("idx_value").cast("double"))
        .withColumn("year", F.year("ref_date"))
        .withColumn("month", F.month("ref_date"))
    )

    # ----------------------------------------------------------------------
    # 1) Snapshot atual (última ref_date por idx_type)
    # ----------------------------------------------------------------------
    w_last = Window.partitionBy("idx_type").orderBy(F.col("ref_date").desc())

    current = (
        silver
        .withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .select(
            "idx_type",
            F.col("ref_date").alias("last_update"),
            F.col("idx_value").alias("current_value"),
            F.col("year").alias("cur_year"),
            F.col("month").alias("cur_month"),
        )
    )

    # ----------------------------------------------------------------------
    # 2) Valor do mês anterior (para MTD)
    # ----------------------------------------------------------------------
    prev_month_ref = (
        current
        .withColumn(
            "prev_month",
            F.when(F.col("cur_month") == 1, F.lit(12)).otherwise(F.col("cur_month") - 1)
        )
        .withColumn(
            "prev_month_year",
            F.when(F.col("cur_month") == 1, F.col("cur_year") - 1).otherwise(F.col("cur_year"))
        )
        .select(
            "idx_type",
            "prev_month",
            "prev_month_year",
        )
    )

    # Último valor dentro de (idx_type, year, month)
    w_prev_month_last = (
        Window
        .partitionBy("idx_type", "year", "month")
        .orderBy(F.col("ref_date").desc())
    )

    prev_month_values = (
        silver
        .withColumn("rn_pm", F.row_number().over(w_prev_month_last))
        .filter(F.col("rn_pm") == 1)
        .select(
            "idx_type",
            F.col("year").alias("pm_year"),
            F.col("month").alias("pm_month"),
            F.col("idx_value").alias("idx_value_prev_month"),
        )
    )

    current_with_prev_month = (
        current
        .join(
            prev_month_ref,
            on="idx_type",
            how="left"
        )
        .join(
            prev_month_values,
            (current.idx_type == prev_month_values.idx_type)
            & (F.col("pm_year") == F.col("prev_month_year"))
            & (F.col("pm_month") == F.col("prev_month")),
            how="left"
        )
        .drop(prev_month_values.idx_type)
        .drop("pm_year", "pm_month")
    )

    # ----------------------------------------------------------------------
    # 3) Valor do ano anterior (para YTD)
    # ----------------------------------------------------------------------
    prev_year_ref = (
        current
        .withColumn("prev_year", F.col("cur_year") - 1)
        .select("idx_type", "prev_year")
    )

    w_prev_year_last = (
        Window
        .partitionBy("idx_type", "year")
        .orderBy(F.col("ref_date").desc())
    )

    prev_year_values = (
        silver
        .withColumn("rn_py", F.row_number().over(w_prev_year_last))
        .filter(F.col("rn_py") == 1)
        .select(
            "idx_type",
            F.col("year").alias("py_year"),
            F.col("idx_value").alias("idx_value_prev_year"),
        )
    )

    current_with_prev = (
        current_with_prev_month
        .join(
            prev_year_ref,
            on="idx_type",
            how="left"
        )
        .join(
            prev_year_values,
            (current_with_prev_month.idx_type == prev_year_values.idx_type)
            & (F.col("py_year") == F.col("prev_year")),
            how="left"
        )
        .drop(prev_year_values.idx_type)
        .drop("py_year")
    )

    # ----------------------------------------------------------------------
    # 4) Calcular mtd_change e ytd_change
    # ----------------------------------------------------------------------
    result = (
        current_with_prev
        .withColumn(
            "mtd_change",
            F.when(
                (F.col("idx_value_prev_month").isNotNull())
                & (F.col("idx_value_prev_month") != 0),
                (F.col("current_value") - F.col("idx_value_prev_month"))
                / F.col("idx_value_prev_month")
            )
        )
        .withColumn(
            "ytd_change",
            F.when(
                (F.col("idx_value_prev_year").isNotNull())
                & (F.col("idx_value_prev_year") != 0),
                (F.col("current_value") - F.col("idx_value_prev_year"))
                / F.col("idx_value_prev_year")
            )
        )
        .select(
            "idx_type",
            "current_value",
            "mtd_change",
            "ytd_change",
            F.lit(None).cast("string").alias("trend_6m"),
            "last_update",
        )
        .dropDuplicates(["idx_type"])
    )

    return result


# ============================================================================
# OURO - tb_mkt_idx_fut_perf
# ============================================================================

@dlt.table(
    name=obter_nome_tabela("ouro", "tb_mkt_idx_fut_perf"),
    comment=obter_metadados_tabela("ouro", "tb_mkt_idx_fut_perf")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["ouro"],
)
def ouro_tb_mkt_idx_fut_perf() -> DataFrame:
    """
    Métricas de performance para índices globais
    a partir de tb_mkt_idx_fut_day.

    - monthly_return: retorno acumulado nos últimos ~30 pregões
    - ytd_return: retorno acumulado desde o 1º dia útil do ano
    - volatility: desvio padrão dos retornos diários em ~30 pregões
    """
    silver = dlt.read(obter_nome_tabela("prata", "tb_mkt_idx_fut_day"))

    silver = (
        silver
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumn("close_price", F.col("close_price").cast("double"))
        .withColumn("year", F.year("trade_date"))
    )

    # Janelas
    w_ret = Window.partitionBy("symbol").orderBy("trade_date")
    w_30d = Window.partitionBy("symbol").orderBy("trade_date").rowsBetween(-29, 0)
    w_ytd = (
        Window
        .partitionBy("symbol", "year")
        .orderBy("trade_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_all = (
        Window
        .partitionBy("symbol")
        .orderBy("trade_date")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    # Retorno diário
    silver = silver.withColumn(
        "ret_diario",
        F.log("close_price") - F.log(F.lag("close_price", 1).over(w_ret))
    )

    silver = (
        silver
        .withColumn("n_obs_30d", F.count("ret_diario").over(w_30d))
        .withColumn("n_obs_ytd", F.count("ret_diario").over(w_ytd))
    )

    # Última data por símbolo
    silver = silver.withColumn(
        "last_update",
        F.max("trade_date").over(Window.partitionBy("symbol"))
    )

    df = (
        silver
        .withColumn(
            "monthly_return",
            F.when(
                F.col("n_obs_30d") >= 2,
                F.exp(F.sum("ret_diario").over(w_30d)) - F.lit(1.0)
            )
        )
        .withColumn(
            "ytd_return",
            F.when(
                F.col("n_obs_ytd") >= 1,
                F.exp(F.sum("ret_diario").over(w_ytd)) - F.lit(1.0)
            )
        )
        .withColumn(
            "volatility",
            F.when(
                F.col("n_obs_30d") >= 2,
                F.stddev("ret_diario").over(w_30d)
            )
        )
        .withColumn("avg_price_30d", F.avg("close_price").over(w_30d))
        .withColumn("last_close", F.last("close_price", ignorenulls=True).over(w_all))
        .withColumn("price_52w_high", F.last("price_52w_high", ignorenulls=True).over(w_all))
        .withColumn("price_52w_low", F.last("price_52w_low", ignorenulls=True).over(w_all))
        .withColumn(
            "pct_from_52w_high",
            F.when(
                (F.col("price_52w_high").isNotNull()) & (F.col("price_52w_high") != 0),
                (F.col("last_close") - F.col("price_52w_high")) / F.col("price_52w_high")
            )
        )
        .withColumn(
            "pct_from_52w_low",
            F.when(
                (F.col("price_52w_low").isNotNull()) & (F.col("price_52w_low") != 0),
                (F.col("last_close") - F.col("price_52w_low")) / F.col("price_52w_low")
            )
        )
        # Mantém só o snapshot mais recente por símbolo
        .filter(F.col("trade_date") == F.col("last_update"))
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
        .dropDuplicates(["symbol"])
    )

    return df
