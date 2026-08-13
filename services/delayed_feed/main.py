"""Delayed feed service — buffers prices and releases after configured delay."""

import logging
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings
from mds_common.kafka.client import consume_loop, create_consumer, create_producer, publish
from mds_common.schemas.events import DelayedPrice
from mds_common.topics import DELAYED_PRICES, NORMALIZED_L1

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

delay = timedelta(minutes=settings.delayed_feed_minutes)
buffer: deque[tuple[datetime, dict]] = deque()
producer = create_producer(settings.kafka_bootstrap_servers)


def release_due_events() -> None:
    now = datetime.now(timezone.utc)
    while buffer and buffer[0][0] <= now:
        _, data = buffer.popleft()
        delayed = DelayedPrice(
            symbol=data["symbol"],
            exchange=data["exchange"],
            price=Decimal(str(data.get("last_price") or data.get("bid_price") or 0)),
            time=now,
            original_time=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            delay_minutes=settings.delayed_feed_minutes,
        )
        publish(producer, DELAYED_PRICES, delayed.symbol, delayed)
        logger.debug("Released delayed price for %s", delayed.symbol)
    producer.flush()


def handle_message(topic: str, data: dict) -> None:
    if topic != NORMALIZED_L1:
        return
    release_at = datetime.now(timezone.utc) + delay
    buffer.append((release_at, data))


def run() -> None:
    consumer = create_consumer(
        settings.kafka_bootstrap_servers,
        group_id="delayed-feed",
        topics=[NORMALIZED_L1],
    )
    logger.info("Delayed feed active — %d minute delay", settings.delayed_feed_minutes)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                import json

                data = json.loads(msg.value().decode("utf-8"))
                handle_message(msg.topic(), data)
            release_due_events()
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Delayed feed stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
