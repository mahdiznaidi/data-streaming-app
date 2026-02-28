"""Cleaning and aggregation transforms for clean_trades streaming records."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_trades(df: DataFrame, watermark: str) -> DataFrame:
    """Cast fields, enforce positive values, and apply watermark + dedup."""
    cleaned = (
        df.withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("trade_value_usdt", F.col("trade_value_usdt").cast("double"))
        .withColumn("timestamp_ms", F.col("timestamp_ms").cast("long"))
        .withColumn("trade_id", F.col("trade_id").cast("long"))
        .withColumn("symbol", F.upper(F.col("symbol")))
        .filter(
            F.col("symbol").isNotNull()
            & (F.col("symbol") != "")
            & F.col("price").isNotNull()
            & F.col("quantity").isNotNull()
            & F.col("timestamp_ms").isNotNull()
            & (F.col("price") > 0)
            & (F.col("quantity") > 0)
            & (F.col("timestamp_ms") > 0)
        )
    )

    return (
        cleaned.withWatermark("event_time", watermark)
        .dropDuplicates(["symbol", "trade_id"])
    )


def aggregate_tumbling_1m(df: DataFrame) -> DataFrame:
    """1-minute tumbling window metrics by symbol."""
    aggregated = df.groupBy(
        F.window("event_time", "1 minute"),
        F.col("symbol"),
    ).agg(
        F.count(F.lit(1)).alias("trade_count"),
        F.avg("price").alias("avg_price"),
        F.sum("quantity").alias("sum_qty"),
        F.sum("trade_value_usdt").alias("sum_value"),
        F.sum(F.when(F.col("is_price_spike"), F.lit(1)).otherwise(F.lit(0))).alias("spike_count"),
    )

    return (
        aggregated.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("trade_count"),
            F.col("avg_price"),
            F.col("sum_qty"),
            F.col("sum_value"),
            F.col("spike_count"),
        )
        .withColumn("event_date", F.to_date("window_start"))
    )


def aggregate_sliding_5m_1m(df: DataFrame) -> DataFrame:
    """5-minute sliding window (1-minute slide) metrics by symbol."""
    aggregated = df.groupBy(
        F.window("event_time", "5 minutes", "1 minute"),
        F.col("symbol"),
    ).agg(
        F.count(F.lit(1)).alias("trade_count"),
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.sum("trade_value_usdt").alias("sum_value"),
    )

    return (
        aggregated.select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("symbol"),
            F.col("trade_count"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("sum_value"),
        )
        .withColumn("event_date", F.to_date("window_start"))
    )
