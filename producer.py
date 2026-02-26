"""
producer.py
-----------
Connects to the Binance WebSocket API and streams real-time
trade data for BTC/USDT and ETH/USDT into Kafka.

Topics produced:
  - raw_trades    : raw messages straight from Binance
  - clean_trades  : validated & cleaned messages ready for Spark
"""

import json
import time
import logging
import threading
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC       = "raw_trades"
CLEAN_TOPIC     = "clean_trades"

# Binance combined stream: BTC/USDT + ETH/USDT trade events
BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@trade/ethusdt@trade"
)

REQUIRED_FIELDS = {"e", "s", "p", "q", "T", "m"}   # event, symbol, price, qty, time, buyer_maker


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create a KafkaProducer, retrying until Kafka is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",           # wait for all replicas to ack
                retries=3,
                linger_ms=5,          # small batch window for throughput
            )
            log.info("✅ Connected to Kafka on %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("⏳ Kafka not ready (attempt %d/%d). Retrying in %ds…", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to Kafka after multiple attempts.")


# ── Data Cleaning & Validation ───────────────────────────────────────────────
def clean_trade(raw: dict) -> dict | None:
    """
    Validate and clean a raw Binance trade message.

    Returns a clean dict ready for Spark, or None if the record is invalid.

    Schema produced:
        symbol       str   e.g. "BTCUSDT"
        price        float trade price in USDT
        quantity     float trade quantity
        trade_time   str   ISO-8601 UTC timestamp
        timestamp_ms int   original epoch milliseconds
        is_buyer_maker bool whether the buyer is the market maker
        source       str   always "binance"
    """
    # ── 1. Check all required fields are present ──────────────────────────────
    if not REQUIRED_FIELDS.issubset(raw.keys()):
        missing = REQUIRED_FIELDS - raw.keys()
        log.warning("⚠️  Missing fields %s — record dropped.", missing)
        return None

    # ── 2. Parse & type-check fields ──────────────────────────────────────────
    try:
        price    = float(raw["p"])
        quantity = float(raw["q"])
        ts_ms    = int(raw["T"])
    except (ValueError, TypeError) as exc:
        log.warning("⚠️  Type conversion failed (%s) — record dropped.", exc)
        return None

    # ── 3. Sanity checks ──────────────────────────────────────────────────────
    if price <= 0:
        log.warning("⚠️  Non-positive price (%.6f) — record dropped.", price)
        return None
    if quantity <= 0:
        log.warning("⚠️  Non-positive quantity (%.6f) — record dropped.", quantity)
        return None
    if ts_ms <= 0:
        log.warning("⚠️  Invalid timestamp (%d) — record dropped.", ts_ms)
        return None

    # ── 4. Build clean record ─────────────────────────────────────────────────
    trade_time = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()

    return {
        "symbol":          raw["s"],          # e.g. "BTCUSDT"
        "price":           price,
        "quantity":        quantity,
        "trade_time":      trade_time,         # ISO-8601 UTC
        "timestamp_ms":    ts_ms,
        "is_buyer_maker":  bool(raw["m"]),
        "source":          "binance",
    }


# ── WebSocket callbacks ───────────────────────────────────────────────────────
def make_callbacks(producer: KafkaProducer):
    """Return WebSocket callback functions bound to the given producer."""

    def on_message(ws, message):
        try:
            envelope = json.loads(message)
            raw_trade = envelope.get("data", {})

            # Always publish the raw message
            producer.send(RAW_TOPIC, value=raw_trade)

            # Clean, validate, then publish to clean topic
            clean = clean_trade(raw_trade)
            if clean:
                producer.send(CLEAN_TOPIC, value=clean)
                log.info(
                    "📨 %s  price=%.2f  qty=%.6f",
                    clean["symbol"], clean["price"], clean["quantity"]
                )

        except json.JSONDecodeError as exc:
            log.error("❌ JSON decode error: %s", exc)
        except Exception as exc:
            log.error("❌ Unexpected error in on_message: %s", exc)

    def on_error(ws, error):
        log.error("❌ WebSocket error: %s", error)

    def on_close(ws, close_status_code, close_msg):
        log.warning("🔌 WebSocket closed (code=%s, msg=%s)", close_status_code, close_msg)

    def on_open(ws):
        log.info("🚀 WebSocket connected to Binance. Streaming BTC/USDT & ETH/USDT…")

    return on_message, on_error, on_close, on_open


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    producer = create_producer()

    on_message, on_error, on_close, on_open = make_callbacks(producer)

    # Auto-reconnect loop: if connection drops, reconnect after 5 s
    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_message and on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            log.info("🛑 Producer stopped by user.")
            producer.flush()
            producer.close()
            break
        except Exception as exc:
            log.error("❌ Connection error: %s. Reconnecting in 5s…", exc)
            time.sleep(5)


if __name__ == "__main__":
    run()