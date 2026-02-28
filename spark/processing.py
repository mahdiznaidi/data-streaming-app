"""Cleaning transforms for clean_trades streaming records."""

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
