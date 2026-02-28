"""Validation helper for Kafka -> Spark clean_trades pipeline."""

from __future__ import annotations

import os
import socket
from pathlib import Path


def parse_bootstrap(bootstrap_servers: str) -> tuple[str, int]:
    first = bootstrap_servers.split(",")[0].strip()
    if ":" not in first:
        raise ValueError(f"Invalid bootstrap server format: {first}")
    host, port = first.rsplit(":", 1)
    return host, int(port)


def kafka_socket_up(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def check_topic_exists(bootstrap_servers: str, topic: str) -> bool | None:
    try:
        from kafka.admin import KafkaAdminClient
    except Exception:
        print("[WARN] kafka-python not available for topic check, skipping.")
        return None

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="spark-pipeline-validator",
        )
        topics = set(admin.list_topics())
        admin.close()
        return topic in topics
    except Exception as exc:
        print(f"[WARN] Could not list Kafka topics: {exc}")
        return None


def parquet_preview(output_dir: Path) -> None:
    tumbling_path = output_dir / "tumbling_1m"
    parquet_files = list(tumbling_path.rglob("*.parquet"))
    if not parquet_files:
        print("[INFO] No parquet files yet in tumbling_1m. Run SINK=parquet for 1-2 minutes first.")
        return

    try:
        from pyspark.sql import SparkSession
    except Exception:
        print("[WARN] pyspark not installed; cannot preview parquet.")
        return

    print(f"[INFO] Reading parquet sample from: {tumbling_path}")
    spark = SparkSession.builder.appName("parquet-preview").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(tumbling_path))
    df.printSchema()
    df.show(10, truncate=False)
    spark.stop()


def print_e2e_instructions() -> None:
    print("\nEnd-to-end test instructions:")
    print("1) docker compose up -d")
    print("2) python producer.py")
    print("3) Start Spark for ~2 minutes with console sink:")
    print(
        "   spark-submit --packages "
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/app.py"
    )

    print("\nSuccess criteria:")
    print("- BTCUSDT and ETHUSDT appear in streaming output")
    print("- trade_count and avg_price are continuously updated")
    print("- no price <= 0 and no quantity <= 0 records")
    print("- spike_count appears when producer detects spikes")


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "clean_trades")
    output_dir = Path(os.getenv("OUTPUT_DIR", "./data/spark_out")).resolve()

    host, port = parse_bootstrap(bootstrap)
    up = kafka_socket_up(host, port)
    print(f"[CHECK] Kafka socket {host}:{port} up: {up}")

    topic_exists = check_topic_exists(bootstrap, topic)
    if topic_exists is None:
        print(f"[CHECK] Topic existence for '{topic}': skipped")
    else:
        print(f"[CHECK] Topic '{topic}' exists: {topic_exists}")

    print_e2e_instructions()
    print("\nOptional parquet validation:")
    parquet_preview(output_dir)


if __name__ == "__main__":
    main()
