"""Kafka producer/consumer helpers."""

import json
import logging
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def _serialize(value: Any) -> bytes:
    if isinstance(value, BaseModel):
        payload = value.model_dump(mode="json")
    elif isinstance(value, dict):
        payload = value
    else:
        payload = {"value": value}
    return json.dumps(payload, default=str).encode("utf-8")


def _deserialize(raw: bytes) -> dict[str, Any]:
    return json.loads(raw.decode("utf-8"))


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers, "linger.ms": 5})


def create_consumer(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    auto_offset_reset: str = "earliest",
) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(topics)
    return consumer


def delivery_report(err: KafkaError | None, msg: Any) -> None:
    if err is not None:
        logger.error("Delivery failed: %s", err)


def publish(producer: Producer, topic: str, key: str | None, value: Any) -> None:
    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if key else None,
        value=_serialize(value),
        callback=delivery_report,
    )
    producer.poll(0)


def consume_loop(
    consumer: Consumer,
    handler: Callable[[str, dict[str, Any]], None],
    poll_timeout: float = 1.0,
) -> None:
    try:
        while True:
            msg = consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            handler(msg.topic(), _deserialize(msg.value()))
    except KeyboardInterrupt:
        logger.info("Consumer interrupted, shutting down")
    finally:
        consumer.close()
