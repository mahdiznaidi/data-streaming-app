"""
producer.py
-----------
Connects to the Binance WebSocket API and streams real-time
trade data for BTC/USDT and ETH/USDT into Kafka.

Data Treatment Pipeline (applied to every message):
  1. Field presence validation
  2. Type conversion & sanity checks
  3. Deduplication (by trade ID)
  4. Price spike detection (flags abnormal price jumps > 2%)
  5. Enrichment (trade_value_usdt, ISO timestamp, spike flag, trade_id)
  6. Per-symbol live stats tracking

Topics produced:
  - raw_trades   : raw messages straight from Binance
  - clean_trades : fully treated & enriched records ready for Spark
"""

import json
import time
import logging
import collections
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC       = "raw_trades"
CLEAN_TOPIC     = "clean_trades"

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    "btcusdt@trade/ethusdt@trade"
)

REQUIRED_FIELDS     = {"e", "s", "p", "q", "T", "m", "t"}
SPIKE_THRESHOLD_PCT = 2.0   # flag if price moves more than 2% in one trade
STATS_LOG_INTERVAL  = 100   # print summary every N clean messages

# ── Deduplication Cache ───────────────────────────────────────────────────────
# Keeps the last 2000 trade IDs per symbol — old IDs auto-evicted (memory safe)
DEDUP_CACHE: dict = collections.defaultdict(lambda: collections.deque(maxlen=2000))

# ── Price Spike Detection ─────────────────────────────────────────────────────
LAST_PRICE: dict = {}

# ── Live Stats Tracker ────────────────────────────────────────────────────────
class SymbolStats:
    def __init__(self):
        self.count       = 0
        self.total_price = 0.0
        self.total_value = 0.0
        self.min_price   = float("inf")
        self.max_price   = float("-inf")
        self.duplicates  = 0
        self.spikes      = 0
        self.dropped     = 0

    def update(self, price: float, value: float):
        self.count       += 1
        self.total_price += price
        self.total_value += value
        self.min_price    = min(self.min_price, price)
        self.max_price    = max(self.max_price, price)

    @property
    def avg_price(self):
        return self.total_price / self.count if self.count > 0 else 0.0

STATS: dict = collections.defaultdict(SymbolStats)
message_counter = 0


# ── Kafka ─────────────────────────────────────────────────────────────────────
def create_producer(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=5,
            )
            log.info("✅ Connected to Kafka on %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("⏳ Kafka not ready (attempt %d/%d). Retrying in %ds...", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka.")


# ── Step 1 & 2: Validation & Type Conversion ─────────────────────────────────
def validate_and_parse(raw):
    if not REQUIRED_FIELDS.issubset(raw.keys()):
        missing = REQUIRED_FIELDS - raw.keys()
        log.warning("⚠️  [VALIDATION] Missing fields %s — dropped.", missing)
        return None
    try:
        price    = float(raw["p"])
        quantity = float(raw["q"])
        ts_ms    = int(raw["T"])
        trade_id = int(raw["t"])
    except (ValueError, TypeError) as exc:
        log.warning("⚠️  [VALIDATION] Type error (%s) — dropped.", exc)
        return None
    if price <= 0 or quantity <= 0 or ts_ms <= 0:
        log.warning("⚠️  [VALIDATION] Non-positive value — dropped.")
        return None
    return raw["s"], price, quantity, ts_ms, bool(raw["m"]), trade_id


# ── Step 3: Deduplication ─────────────────────────────────────────────────────
def is_duplicate(symbol, trade_id):
    cache = DEDUP_CACHE[symbol]
    if trade_id in cache:
        STATS[symbol].duplicates += 1
        log.warning("⚠️  [DEDUP] Duplicate trade ID %d for %s — dropped.", trade_id, symbol)
        return True
    cache.append(trade_id)
    return False


# ── Step 4: Price Spike Detection ────────────────────────────────────────────
def detect_spike(symbol, price):
    is_spike = False
    if symbol in LAST_PRICE:
        last = LAST_PRICE[symbol]
        pct_change = abs(price - last) / last * 100
        if pct_change > SPIKE_THRESHOLD_PCT:
            STATS[symbol].spikes += 1
            log.warning("🚨 [SPIKE] %s jumped %.4f%% (%.2f -> %.2f)", symbol, pct_change, last, price)
            is_spike = True
    LAST_PRICE[symbol] = price
    return is_spike


# ── Step 5: Enrichment ───────────────────────────────────────────────────────
def enrich(symbol, price, quantity, ts_ms, is_buyer_maker, trade_id, is_spike):
    return {
        "symbol":           symbol,
        "price":            price,
        "quantity":         quantity,
        "trade_value_usdt": round(price * quantity, 6),
        "trade_time":       datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
        "timestamp_ms":     ts_ms,
        "is_buyer_maker":   is_buyer_maker,
        "is_price_spike":   is_spike,
        "trade_id":         trade_id,
        "source":           "binance",
    }


# ── Step 6: Stats Summary ────────────────────────────────────────────────────
def log_stats():
    log.info("=" * 65)
    log.info("📊 LIVE STATS SUMMARY")
    for symbol, s in STATS.items():
        log.info(
            "  %s | trades=%d | avg_price=%.2f | min=%.2f | max=%.2f | "
            "total_vol=%.4f USDT | dupes=%d | spikes=%d",
            symbol, s.count, s.avg_price, s.min_price, s.max_price,
            s.total_value, s.duplicates, s.spikes,
        )
    log.info("=" * 65)


# ── Full Pipeline ────────────────────────────────────────────────────────────
def process_trade(raw):
    parsed = validate_and_parse(raw)
    if parsed is None:
        return None
    symbol, price, quantity, ts_ms, is_buyer_maker, trade_id = parsed

    if is_duplicate(symbol, trade_id):
        return None

    is_spike = detect_spike(symbol, price)
    clean    = enrich(symbol, price, quantity, ts_ms, is_buyer_maker, trade_id, is_spike)
    STATS[symbol].update(price, clean["trade_value_usdt"])
    return clean


# ── WebSocket Callbacks ──────────────────────────────────────────────────────
def make_callbacks(producer):
    global message_counter

    def on_message(ws, message):
        global message_counter
        try:
            raw_trade = json.loads(message).get("data", {})
            producer.send(RAW_TOPIC, value=raw_trade)

            clean = process_trade(raw_trade)
            if clean:
                producer.send(CLEAN_TOPIC, value=clean)
                message_counter += 1
                log.info(
                    "📨 %s  price=%.2f  qty=%.6f  value=%.4f USDT  %s",
                    clean["symbol"], clean["price"], clean["quantity"],
                    clean["trade_value_usdt"],
                    "🚨 SPIKE!" if clean["is_price_spike"] else "",
                )
                if message_counter % STATS_LOG_INTERVAL == 0:
                    log_stats()

        except json.JSONDecodeError as exc:
            log.error("❌ JSON error: %s", exc)
        except Exception as exc:
            log.error("❌ Error: %s", exc)

    def on_error(ws, error):
        log.error("❌ WebSocket error: %s", error)

    def on_close(ws, code, msg):
        log.warning("🔌 WebSocket closed. Final stats:")
        log_stats()

    def on_open(ws):
        log.info("🚀 Connected to Binance. Streaming BTC/USDT & ETH/USDT...")
        log.info("🔍 Pipeline: validation → dedup → spike detection → enrichment → stats")

    return on_message, on_error, on_close, on_open


# ── Main ─────────────────────────────────────────────────────────────────────
def run():
    producer = create_producer()
    on_message, on_error, on_close, on_open = make_callbacks(producer)

    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except KeyboardInterrupt:
            log.info("🛑 Stopped.")
            producer.flush()
            producer.close()
            log_stats()
            break
        except Exception as exc:
            log.error("❌ %s. Reconnecting in 5s...", exc)
            time.sleep(5)


if __name__ == "__main__":
    run()