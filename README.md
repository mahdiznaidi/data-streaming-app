# data-streaming-app

Real-time crypto pipeline:
- `producer.py` (Molka): Binance WebSocket -> Kafka (`raw_trades`, `clean_trades`)
- `spark/` (Mehdi): Spark Structured Streaming consuming `clean_trades`

## Spark job (Mehdi)

### Source

Spark reads Kafka topic `clean_trades` by default:

- `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
- `KAFKA_TOPIC=clean_trades`
- `KAFKA_STARTING_OFFSETS=latest`

Kafka source in code:

```python
spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
  .selectExpr("CAST(value AS STRING) AS json_str")
```

### Expected clean_trades schema

```json
{
  "symbol": "BTCUSDT",
  "price": 43123.42,
  "quantity": 0.012,
  "trade_value_usdt": 517.48104,
  "trade_time": "2026-02-28T15:00:00.123000+00:00",
  "timestamp_ms": 1740754800123,
  "is_buyer_maker": false,
  "is_price_spike": true,
  "trade_id": 123456789,
  "source": "binance"
}
```

### Cleaning and state handling

- Schema parsing with `from_json(..., CLEAN_TRADE_SCHEMA)`
- `event_time` derived from `timestamp_ms`
- Numeric casts for `price`, `quantity`, `trade_value_usdt`
- Filter invalid rows (`symbol`, positive `price/quantity`, positive `timestamp_ms`)
- Watermark: `withWatermark("event_time", WATERMARK)` (default `2 minutes`)
- Streaming deduplication after watermark: `dropDuplicates(["symbol","trade_id"])`

### Windowed aggregations

1. Tumbling `1 minute` by symbol:
- `trade_count`
- `avg_price`
- `sum_qty`
- `sum_value`
- `spike_count`

2. Sliding `5 minutes` / slide `1 minute` by symbol:
- `trade_count`
- `avg_price`
- `min_price`
- `max_price`
- `sum_value`

### Sinks

- `SINK=console` (default): 2 streaming queries in `outputMode("update")`
- `SINK=parquet`: 2 streaming queries in `outputMode("append")`
  - `./data/spark_out/tumbling_1m/`
  - `./data/spark_out/sliding_5m_1m/`
  - Checkpoints:
    - `./data/checkpoints/tumbling_1m/`
    - `./data/checkpoints/sliding_5m_1m/`

### Data handoff for Nour (Dashboard/ML)

When Spark runs with `SINK=parquet`, the data to consume is:

- `./data/spark_out/tumbling_1m/`
- `./data/spark_out/sliding_5m_1m/`

Inside each folder, use the parquet files (`*.parquet`) as dataset source.
Checkpoint folders are internal Spark state and should not be consumed by dashboard/ML code.

Quick read example (PySpark):

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
tumbling = spark.read.parquet("data/spark_out/tumbling_1m")
sliding = spark.read.parquet("data/spark_out/sliding_5m_1m")
```

More details are in [`README_NOUR.md`](README_NOUR.md).

## Run

1. Install Python deps:

```bash
pip install -r requirements.txt
pip install -r spark/requirements.txt
```

2. Start Kafka infra:

```bash
docker compose up -d
```

3. Start producer:

```bash
python producer.py
```

4. Start Spark job:

```bash
python -m spark.app
```

If Spark cannot load Kafka connector, run with package:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/app.py
```

If your Spark version is not `3.5.1`, replace package version accordingly:

`org.apache.spark:spark-sql-kafka-0-10_2.12:<your_spark_version>`

## Validation helper

Use:

```bash
python tools/validate_pipeline.py
```

The script:
- checks Kafka socket availability (`localhost:9092` by default)
- optionally checks topic existence
- prints end-to-end steps and success criteria
- optionally previews parquet output if files are present
