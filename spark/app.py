"""Spark Structured Streaming entrypoint for Kafka clean_trades consumption."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from spark.config import Settings, get_settings
from spark.processing import clean_trades
from spark.schemas import parse_clean_trades


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("spark.app")


def create_spark_session(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.spark_master)
        .getOrCreate()
    )


def read_clean_trades_from_kafka(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", settings.kafka_starting_offsets)
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str")
    )


def main() -> None:
    settings = get_settings()
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    LOGGER.info(
        "Spark streaming start | bootstrap=%s topic=%s sink=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.sink,
    )

    raw_df = read_clean_trades_from_kafka(spark, settings)
    parsed_df = parse_clean_trades(raw_df)
    cleaned_df = clean_trades(parsed_df, watermark=settings.watermark)

    # Commit 1 scope: verify Kafka consumption and transformed events in console.
    query = (
        cleaned_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
