"""Configuration helpers for Spark clean_trades streaming job."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


@dataclass(frozen=True)
class Settings:
    app_name: str
    spark_master: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_starting_offsets: str
    watermark: str
    sink: str
    output_dir: Path
    checkpoint_dir: Path


def get_settings() -> Settings:
    return Settings(
        app_name=_env("SPARK_APP_NAME", "clean-trades-streaming"),
        spark_master=_env("SPARK_MASTER", "local[*]"),
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=_env("KAFKA_TOPIC", "clean_trades"),
        kafka_starting_offsets=_env("KAFKA_STARTING_OFFSETS", "latest"),
        watermark=_env("WATERMARK", "2 minutes"),
        sink=_env("SINK", "console").lower(),
        output_dir=Path(_env("OUTPUT_DIR", "./data/spark_out")).resolve(),
        checkpoint_dir=Path(_env("CHECKPOINT_DIR", "./data/checkpoints")).resolve(),
    )
