"""
consumer.py
-----------
Reads cleaned trade messages from the 'clean_trades' Kafka topic.
Windows-compatible version (fixes selector bug on Python 3.12).

Usage:
    python consumer.py
    python consumer.py --topic raw_trades
    python consumer.py --limit 50
"""

import json
import argparse
import logging
import time

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.WARNING,  # suppress kafka internal logs
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC   = "clean_trades"


def run(topic: str, limit):
    print(f"\n{'='*70}")
    print(f"  Consuming from topic: '{topic}'   (Ctrl+C to stop)")
    print(f"{'='*70}\n")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="member1-verify-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,   # don't block forever, loop instead
    )

    count = 0
    print("Waiting for messages...\n")

    try:
        while True:
            for message in consumer:
                count += 1
                msg = message.value
                symbol   = msg.get("symbol", "?")
                price    = msg.get("price", 0)
                quantity = msg.get("quantity", 0)
                value    = msg.get("trade_value_usdt", 0)
                ts       = msg.get("trade_time", "?")
                spike    = "🚨 SPIKE" if msg.get("is_price_spike") else ""

                print(
                    f"[{count:>5}] {symbol:<10} "
                    f"price={price:>12.2f} USDT  "
                    f"qty={quantity:>12.6f}  "
                    f"value={value:>10.4f} USDT  "
                    f"{spike}"
                )

                if limit and count >= limit:
                    print(f"\n✅ Reached limit of {limit} messages.")
                    return

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"\n📊 Total messages consumed: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(args.topic, args.limit)