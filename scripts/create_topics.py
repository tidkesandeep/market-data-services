"""Create Kafka topics in Redpanda."""

import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings
from mds_common.topics import ALL_TOPICS


def main() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics

    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1)
        for topic in ALL_TOPICS
        if topic not in existing
    ]

    if not new_topics:
        print("All topics already exist.")
        return

    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic: {topic}")
        except Exception as exc:
            print(f"Topic {topic}: {exc}")


if __name__ == "__main__":
    main()
