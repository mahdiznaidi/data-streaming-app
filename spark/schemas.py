"""Schemas and parsing helpers for Kafka clean_trades messages."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


CLEAN_TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_value_usdt", DoubleType(), True),
        StructField("trade_time", StringType(), True),
        StructField("timestamp_ms", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("is_price_spike", BooleanType(), True),
        StructField("trade_id", LongType(), True),
        StructField("source", StringType(), True),
    ]
)


def parse_clean_trades(df: DataFrame) -> DataFrame:
    """Parse json_str from Kafka records and project clean_trades fields."""
    return (
        df.select(F.from_json(F.col("json_str"), CLEAN_TRADE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("symbol", F.upper(F.col("symbol")))
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime((F.col("timestamp_ms") / F.lit(1000)).cast("double"))),
        )
    )
