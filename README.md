# CryptoStream — Real-Time Analytics Pipeline

> **M2 BDIA — Data Streaming Project · 2025/2026**  
> Université Paris Dauphine

---

## 👥 Team

**Molka ESSID**
**Mehdi ZNAIDI** 
**Nour SAHLI** 

---

## 📌 Project Overview

CryptoStream is a fully real-time data streaming application that:

1. **Ingests** live cryptocurrency trade data from the Binance WebSocket API
2. **Processes** the stream through a multi-step data treatment pipeline
3. **Aggregates** trades using Spark Structured Streaming with tumbling and sliding windows
4. **Applies Machine Learning** — anomaly detection and price prediction on the live stream
5. **Visualizes** everything in a live auto-refreshing Streamlit dashboard

All data is real — every message is an actual trade happening on Binance at that exact moment.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BINANCE WEBSOCKET API                        │
│                   (BTC/USDT & ETH/USDT live trades)                 │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    producer.py                       │
│                                                                     │
│  Step 1+2 │ Field validation & type conversion                      │
│  Step 3   │ Deduplication (rolling cache of 2000 trade IDs)        │
│  Step 4   │ Price spike detection (flag if jump > 2%)              │
│  Step 5   │ Enrichment (trade_value_usdt, ISO timestamp, trade_id) │
│  Step 6   │ Live stats tracking (avg/min/max price, volume)        │
└──────┬────────────────────────────┬───────────────────────────────-─┘
       │                            │
       ▼                            ▼
┌─────────────┐            ┌─────────────────┐
│ raw_trades  │            │  clean_trades   │  ← Kafka Topics
│  (Kafka)    │            │   (Kafka)       │
└─────────────┘            └────────┬────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   spark/app.py                       │
│                                                                     │
│  • Schema parsing & type casting                                    │
│  • Watermark (2 min) + deduplication by trade_id                   │
│  • Tumbling window (1 min): trade_count, avg_price,                │
│    sum_qty, sum_value, spike_count                                  │
│  • Sliding window (5 min / 1 min slide): trade_count,              │
│    avg_price, min_price, max_price, sum_value                      │
└──────┬────────────────────────────┬───────────────────────────────-─┘
       │                            │
       ▼                            ▼
┌──────────────────┐     ┌──────────────────────┐
│ tumbling_1m/     │     │ sliding_5m_1m/        │  ← Parquet Files
│ (parquet)        │     │ (parquet)             │
└────────┬─────────┘     └──────────┬────────────┘
         │                          │
         └──────────┬───────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│               dashboard.py + ml_model.py           │
│                                                                     │
│  • Isolation Forest — anomaly detection on streaming windows       │
│  • Linear Regression — next window price prediction                │
│  • Streamlit dashboard — 4 tabs, live KPIs, auto-refresh           │
│  • Charts: price, volume, spikes, sliding range, anomalies         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
data-streaming-app/
│
├── producer.py              # Binance WebSocket → Kafka (Molka)
├── consumer.py              # Verification consumer
├── setup_topics.py          # Kafka topic creation utility
├── docker-compose.yml       # Kafka + Zookeeper + Kafka UI
├── requirements.txt         # Python dependencies (ingestion)
│
├── spark/                   # Spark Structured Streaming (Mehdi)
│   ├── app.py               # Streaming entrypoint
│   ├── config.py            # Environment-based configuration
│   ├── processing.py        # Cleaning + windowed aggregations
│   ├── schemas.py           # Kafka message schema
│   └── requirements.txt     # Spark dependencies
│
├── dashboard.py             # Streamlit dashboard (Nour)
├── ml_model.py              # Isolation Forest + Linear Regression
├── data_loader.py           # Parquet reader with caching
├── requirements_dashboard.txt
│
├── data/
│   └── spark_out/
│       ├── tumbling_1m/     # Spark parquet output (1-min windows)
│       └── sliding_5m_1m/  # Spark parquet output (5-min windows)
│
├── tools/
│   └── validate_pipeline.py # End-to-end pipeline validation helper
│
├── README.md                # This file
└── README_NOUR.md           # Spark output schema for ML/dashboard
```

---

## 🔧 Technologies Used

| Layer | Technology |
|-------|-----------|
| Data Source | Binance WebSocket API (real-time, free, no auth) |
| Message Broker | Apache Kafka + Zookeeper (via Docker) |
| Stream Processing | Apache Spark Structured Streaming 3.5 |
| Machine Learning | scikit-learn (Isolation Forest, Linear Regression) |
| Visualization | Streamlit + Plotly |
| Language | Python 3.12 |
| Infrastructure | Docker + Docker Compose |

---

## 🚀 How to Run

### Prerequisites
- Docker & Docker Compose installed
- Python 3.10+
- Java 8+ (required for Spark)

### Step 1 — Start Kafka

```bash
docker-compose up -d
```

Wait ~20 seconds. Kafka UI available at: http://localhost:8080

### Step 2 — Install dependencies

```bash
pip install -r requirements.txt
pip install -r spark/requirements.txt
pip install -r requirements_dashboard.txt
```

### Step 3 — Create Kafka topics

```bash
python setup_topics.py
```

### Step 4 — Start the producer (Terminal 1)

```bash
python producer.py
```

### Step 5 — Start Spark with parquet sink (Terminal 2)

**Linux / Mac:**
```bash
SINK=parquet python -m spark.app
```

**Windows (PowerShell):**
```powershell
$env:SINK="parquet"
python -m spark.app
```

> ⚠️ Must be run from the project root (`data-streaming-app/`), not from inside `spark/`

If Spark cannot load the Kafka connector:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark/app.py
```

### Step 6 — Launch the dashboard (Terminal 3)

```bash
streamlit run dashboard.py
```

Open: **http://localhost:8501**

> Wait 1-2 minutes after starting Spark for the first parquet files to appear.

---

## 📊 Kafka Topics

| Topic | Description |
|-------|-------------|
| `raw_trades` | Raw JSON messages from Binance, no transformation |
| `clean_trades` | Validated, deduplicated, enriched records for Spark |

### Clean trade schema

```json
{
  "symbol":           "BTCUSDT",
  "price":            67432.10,
  "quantity":         0.00120,
  "trade_value_usdt": 80.92,
  "trade_time":       "2026-02-28T16:57:00.123000+00:00",
  "timestamp_ms":     1740754800123,
  "is_buyer_maker":   false,
  "is_price_spike":   false,
  "trade_id":         123456789,
  "source":           "binance"
}
```

---

## 🤖 Machine Learning

### Model 1 — Anomaly Detection (Isolation Forest)

Detects unusual market behavior across 1-minute windows.

**Features:** `avg_price`, `trade_count`, `sum_value`, `price_change_pct`, `value_per_trade`, `spike_rate`

**Output:** `anomaly` (bool) + `anomaly_score` (float, lower = more anomalous)

**Contamination rate:** 5% — flags the most abnormal windows

### Model 2 — Price Prediction (Linear Regression)

Predicts the next 1-minute window's average price.

**Features:** `rolling_mean_5`, `rolling_mean_10`, `rolling_std_5`, `price_change`, `price_change_pct`, `volume_zscore`

**Output:** `predicted_price` — next expected average price with direction (📈 / 📉)

Both models are **automatically retrained** on every dashboard refresh as new Spark data arrives.

---

## 📈 Dashboard Features

| Tab | Content |
|-----|---------|
| **Aggregations** | Price over time, trade count, volume, spike count, sliding window range |
| **ML Predictions** | Actual vs predicted price chart, prediction error chart |
| **Anomaly Detection** | Per-symbol anomaly chart, anomaly score over time, anomaly table |
| **Raw Data** | Full tumbling and sliding window dataframes |

Live KPIs: current price, next predicted price, total trades, total volume, ML anomaly status.

Auto-refreshes every N seconds (configurable in sidebar, default 15s).

---

## ✅ Validation

Run the pipeline validation helper to check everything is connected:

```bash
python tools/validate_pipeline.py
```
**28 February 2026** — Submitted via GitHub  
Course: Data Streaming · M2 BDIA · Université Paris Dauphine  
Professor: Nour ElHouda Ben Ali
