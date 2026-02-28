# README Nour

This file explains where Spark outputs are located and how to read them for
dashboard/forecasting tasks.

## Where the data is

From this repository root (`data-streaming-app/`), Spark writes parquet outputs to:

- `data/spark_out/tumbling_1m/`
- `data/spark_out/sliding_5m_1m/`

Spark checkpoints are stored in:

- `data/checkpoints/tumbling_1m/`
- `data/checkpoints/sliding_5m_1m/`

Use only `data/spark_out/...` for analytics/ML. Checkpoints are for Spark recovery only.

## What each dataset contains

`tumbling_1m` columns:

- `window_start`
- `window_end`
- `symbol`
- `trade_count`
- `avg_price`
- `sum_qty`
- `sum_value`
- `spike_count`
- `event_date`

`sliding_5m_1m` columns:

- `window_start`
- `window_end`
- `symbol`
- `trade_count`
- `avg_price`
- `min_price`
- `max_price`
- `sum_value`
- `event_date`

## Read with PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
tumbling = spark.read.parquet("data/spark_out/tumbling_1m")
sliding = spark.read.parquet("data/spark_out/sliding_5m_1m")
```

If Spark metadata files are incomplete after an interrupted run, read parquet files directly:

```python
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
base = Path("data/spark_out")
tumbling_files = [str(p) for p in (base / "tumbling_1m").rglob("*.parquet")]
sliding_files = [str(p) for p in (base / "sliding_5m_1m").rglob("*.parquet")]

tumbling = spark.read.parquet(*tumbling_files)
sliding = spark.read.parquet(*sliding_files)
```

## Read with Pandas

```python
import pandas as pd

tumbling = pd.read_parquet("data/spark_out/tumbling_1m")
sliding = pd.read_parquet("data/spark_out/sliding_5m_1m")
```

## Run mode needed to produce files

To generate/update these files:

1. Start Kafka + producer (`docker compose up -d` then `python producer.py`).
2. Start Spark with `SINK=parquet`.
3. Let it run 1-2 minutes minimum before reading outputs.
