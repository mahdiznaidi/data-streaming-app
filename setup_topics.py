"""
setup_topics.py
---------------
Creates the required Kafka topics before starting the producer.
Run this ONCE after docker-compose is up.

Usage:
    python setup_topics.py
"""

import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"

TOPICS = [
    NewTopic(name="raw_trades",   num_partitions=2, replication_factor=1),
    NewTopic(name="clean_trades", num_partitions=2, replication_factor=1),
]


def main():
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    for topic in TOPICS:
        try:
            admin.create_topics([topic])
            log.info("✅ Topic '%s' created.", topic.name)
        except TopicAlreadyExistsError:
            log.info("ℹ️  Topic '%s' already exists — skipping.", topic.name)
    admin.close()
    log.info("🎉 All topics ready.")


if __name__ == "__main__":
    main()