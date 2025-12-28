"""Camada Prata do pipeline Delta Live Tables."""

from __future__ import annotations

import dlt
from pyspark.sql import DataFrame, functions as F

from utilitarios.configuracoes import (
    PROPRIEDADES_TABELAS,
    obter_nome_tabela,
    obter_metadados_tabela,
)

NOME_BRONZE_COTACOES = obter_nome_tabela("bronze", "cotacoes_b3")
NOME_BRONZE_BACEN = obter_nome_tabela("bronze", "series_bacen")
NOME_BRONZE_INDICES_FUTUROS = obter_nome_tabela("bronze", "indices_futuros")

NOME_PRATA_COTACOES = obter_nome_tabela("prata", "tb_mkt_eqt_day")
NOME_PRATA_BACEN = obter_nome_tabela("prata", "tb_mkt_idx_eco")
NOME_PRATA_IDX_FUT = obter_nome_tabela("prata", "tb_mkt_idx_fut_day")


def normalizar_colunas(df: DataFrame) -> DataFrame:
    """Padroniza o nome das colunas para minúsculo com underscores."""
    return df.select(
        [
            F.col(coluna).alias(coluna.lower().replace(" ", "_"))
            for coluna in df.columns
        ]
    )


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_eqt_day"),
    comment=obter_metadados_tabela("prata", "tb_mkt_eqt_day")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"],
)
@dlt.expect_all(
    {
        "valid_close": "close_price IS NOT NULL",
        "valid_date": "trade_date IS NOT NULL",
    }
)
def prata_cotacoes_b3() -> DataFrame:
    """Normaliza e valida as cotações de equity coletadas na camada Bronze."""
    
    df = dlt.read(NOME_BRONZE_COTACOES)
    df_norm = normalizar_colunas(df)
    return df_norm.select(
        F.col("date").alias("trade_date"),
        F.col("open").alias("open_price"),
        F.col("high").alias("high_price"),
        F.col("low").alias("low_price"),
        F.col("close").alias("close_price"),
        "volume",
        F.col("dividends").alias("div_cash"),
        F.col("stock_splits").alias("split_ratio"),
        F.col("ticker").alias("symbol"),
        "ingestion_timestamp",
    )


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_idx_eco"),
    comment=obter_metadados_tabela("prata", "tb_mkt_idx_eco")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"],
)
def prata_series_bacen() -> DataFrame:
    """
    Trata as séries do BACEN garantindo consistência de tipos e nomenclatura.
    
    O campo 'frequency' indica a periodicidade do indicador econômico:
    - D: Diário (ex: Taxa Selic diária, CDI)
    - M: Mensal (ex: IPCA, IGP-M)
    - A: Anual (ex: PIB)
    """
    df = dlt.read(NOME_BRONZE_BACEN)
    df_norm = normalizar_colunas(df)
    return df_norm.select(
        F.col("data").alias("ref_date"),
        F.col("valor").cast("double").alias("idx_value"),
        F.col("serie").alias("idx_type"),
        F.lit("D").alias("frequency"),  # D = Diário
        "ingestion_timestamp",
    )


@dlt.table(
    name=obter_nome_tabela("prata", "tb_mkt_idx_fut_day"),
    comment=obter_metadados_tabela("prata", "tb_mkt_idx_fut_day")["descricao"],
    table_properties=PROPRIEDADES_TABELAS["prata"],
)
@dlt.expect_all(
    {
        "valid_close": "close_price IS NOT NULL",
        "valid_symbol": "symbol IS NOT NULL",
        "valid_trade_date": "trade_date IS NOT NULL",
    }
)
def prata_indices_futuros() -> DataFrame:
    """
    Normaliza e enriquece as cotações de derivativos de índices globais.
    
    Origem: bronze.indices_futuros (schema original da API brapi.dev).
    
    Saída: tb_mkt_idx_fut_day
    - trade_datetime: timestamp da cotação (regularMarketTime)
    - trade_date: data de referência (derivada do timestamp)
    - open_price, high_price, low_price, close_price, prev_close_price
    - abs_change, pct_change, volume
    - symbol: identificador lógico (ex: IndSp500)
    - index_symbol: ticker da API (ex: ^GSPC)
    - index_name: nome amigável do índice
    - region, category, currency
    - price_52w_high, price_52w_low
    """
    df = dlt.read(NOME_BRONZE_INDICES_FUTUROS)
    df_norm = normalizar_colunas(df)
    
    return df_norm.select(
        F.col("regularmarkettime").alias("trade_datetime"),
        F.to_date("regularmarkettime").alias("trade_date"),
        # Identificadores
        F.col("indice").alias("symbol"),
        F.col("symbol").alias("index_symbol"),
        F.coalesce(
            F.col("longname"),
            F.col("shortname"),
            F.col("indice"),
        ).alias("index_name"),
        # Taxonomia
        F.col("regiao").alias("region"),
        F.col("categoria").alias("category"),
        F.col("currency"),
        # Métricas de mercado
        F.col("regularmarketopen").alias("open_price"),
        F.col("regularmarketdayhigh").alias("high_price"),
        F.col("regularmarketdaylow").alias("low_price"),
        F.col("regularmarketprice").alias("close_price"),
        F.col("regularmarketpreviousclose").alias("prev_close_price"),
        F.col("regularmarketchange").alias("abs_change"),
        F.col("regularmarketchangepercent").alias("pct_change"),
        F.col("regularmarketvolume").cast("double").alias("volume"),
        F.col("fiftytwoweekhigh").alias("price_52w_high"),
        F.col("fiftytwoweeklow").alias("price_52w_low"),
        "ingestion_timestamp",
    )


__all__ = [
    "normalizar_colunas",
    "prata_cotacoes_b3",
    "prata_series_bacen",
    "prata_indices_futuros",
]
