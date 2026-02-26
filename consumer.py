"""
consumer.py
-----------
Reads cleaned trade messages from the 'clean_trades' Kafka topic
and prints them to the console.

Two purposes:
  1. Verify the producer is working correctly (run this to see live data).
  2. Serves as the base consumer that Member 2 (Spark) will connect to.

Usage:
    python consumer.py
    python consumer.py --topic raw_trades      # to inspect raw messages
    python consumer.py --limit 50              # stop after 50 messages
"""

import json
import argparse
import logging
from datetime import datetime

from kafka import KafkaConsumer
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
DEFAULT_TOPIC   = "clean_trades"


# ── Consumer ─────────────────────────────────────────────────────────────────
def create_consumer(topic: str) -> KafkaConsumer:
    """Create and return a KafkaConsumer for the given topic."""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",          # only new messages
            enable_auto_commit=True,
            group_id="member1-verify-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        log.info("✅ Consumer connected. Listening on topic '%s'…", topic)
        return consumer
    except NoBrokersAvailable:
        raise RuntimeError(
            "❌ Kafka not reachable. Make sure docker-compose is running:\n"
            "   docker-compose up -d"
        )


def print_trade(msg: dict, count: int):
    """Pretty-print a clean trade message."""
    symbol   = msg.get("symbol", "?")
    price    = msg.get("price", 0)
    quantity = msg.get("quantity", 0)
    ts       = msg.get("trade_time", "?")
    maker    = "MAKER" if msg.get("is_buyer_maker") else "TAKER"

    print(
        f"[{count:>5}] {symbol:<10} "
        f"price={price:>12.2f} USDT  "
        f"qty={quantity:>12.6f}  "
        f"{maker:<6}  "
        f"time={ts}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def run(topic: str, limit: int | None):
    consumer = create_consumer(topic)
    count = 0

    print("\n" + "=" * 80)
    print(f"  Consuming from topic: '{topic}'   (Ctrl+C to stop)")
    print("=" * 80 + "\n")

    try:
        for message in consumer:
            count += 1
            print_trade(message.value, count)

            if limit and count >= limit:
                log.info("✅ Reached limit of %d messages. Stopping.", limit)
                break

    except KeyboardInterrupt:
        log.info("🛑 Consumer stopped by user.")
    finally:
        consumer.close()
        log.info("📊 Total messages consumed: %d", count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka trade consumer (verification)")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume")
    parser.add_argument("--limit", type=int, default=None, help="Stop after N messages")
    args = parser.parse_args()

    run(args.topic, args.limit)