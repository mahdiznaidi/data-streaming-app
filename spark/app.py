"""Spark Structured Streaming entrypoint for Kafka clean_trades processing."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from spark.config import Settings, get_settings
from spark.processing import (
    aggregate_sliding_5m_1m,
    aggregate_tumbling_1m,
    clean_trades,
)
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
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str")
    )


def _build_console_query(df: DataFrame, checkpoint_path: Path, query_name: str):
    return (
        df.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", str(checkpoint_path))
        .queryName(query_name)
        .start()
    )


def _build_parquet_query(
    df: DataFrame,
    output_path: Path,
    checkpoint_path: Path,
    query_name: str,
):
    output_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    return (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .queryName(query_name)
        .start()
    )


def start_queries(
    tumbling_df: DataFrame,
    sliding_df: DataFrame,
    settings: Settings,
):
    tumbling_checkpoint = settings.checkpoint_dir / "tumbling_1m"
    sliding_checkpoint = settings.checkpoint_dir / "sliding_5m_1m"

    if settings.sink == "console":
        tumbling_checkpoint.mkdir(parents=True, exist_ok=True)
        sliding_checkpoint.mkdir(parents=True, exist_ok=True)
        q1 = _build_console_query(
            tumbling_df,
            checkpoint_path=tumbling_checkpoint,
            query_name="tumbling_1m_console",
        )
        q2 = _build_console_query(
            sliding_df,
            checkpoint_path=sliding_checkpoint,
            query_name="sliding_5m_1m_console",
        )
        return [q1, q2]

    if settings.sink == "parquet":
        tumbling_out = settings.output_dir / "tumbling_1m"
        sliding_out = settings.output_dir / "sliding_5m_1m"
        q1 = _build_parquet_query(
            tumbling_df,
            output_path=tumbling_out,
            checkpoint_path=tumbling_checkpoint,
            query_name="tumbling_1m_parquet",
        )
        q2 = _build_parquet_query(
            sliding_df,
            output_path=sliding_out,
            checkpoint_path=sliding_checkpoint,
            query_name="sliding_5m_1m_parquet",
        )
        return [q1, q2]

    raise ValueError("SINK must be console or parquet.")


def main() -> None:
    settings = get_settings()
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    LOGGER.info(
        "Spark streaming start | bootstrap=%s topic=%s sink=%s output=%s checkpoints=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.sink,
        settings.output_dir,
        settings.checkpoint_dir,
    )

    raw_df = read_clean_trades_from_kafka(spark, settings)
    parsed_df = parse_clean_trades(raw_df)
    cleaned_df = clean_trades(parsed_df, watermark=settings.watermark)
    tumbling_df = aggregate_tumbling_1m(cleaned_df)
    sliding_df = aggregate_sliding_5m_1m(cleaned_df)

    queries = start_queries(tumbling_df, sliding_df, settings)
    for query in queries:
        LOGGER.info("Started query name=%s id=%s", query.name, query.id)
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
