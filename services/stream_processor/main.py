"""Stream processor — normalizes raw events, writes to DB, caches L1, emits quality events."""

import asyncio
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings
from mds_common.kafka.client import consume_loop, create_consumer, create_producer, publish
from mds_common.metrics import DATA_QUALITY_EVENTS, MESSAGES_PROCESSED, PROCESSING_LATENCY, start_metrics_server
from mds_common.redis.cache import RedisCache
from mds_common.schemas.events import DataQualityEvent, Level1Snapshot, Level2Snapshot, OrderBookLevel
from mds_common.topics import DATA_QUALITY, NORMALIZED_L1, NORMALIZED_L2, RAW_QUOTES, RAW_TRADES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
cache = RedisCache(settings.redis_url)
producer = create_producer(settings.kafka_bootstrap_servers)

import time

last_prices: dict[str, Decimal] = {}


async def persist_trade(data: dict) -> None:
    async with SessionLocal() as session:
        await session.execute(
            text(
                """
                INSERT INTO trades (time, symbol, exchange, price, size, trade_id, side, source)
                VALUES (:time, :symbol, :exchange, :price, :size, :trade_id, :side, :source)
                """
            ),
            data,
        )
        await session.commit()


async def persist_quote(data: dict) -> None:
    async with SessionLocal() as session:
        await session.execute(
            text(
                """
                INSERT INTO quotes (time, symbol, exchange, bid_price, bid_size, ask_price, ask_size, source)
                VALUES (:time, :symbol, :exchange, :bid_price, :bid_size, :ask_price, :ask_size, :source)
                """
            ),
            data,
        )
        await session.commit()


def validate_price(symbol: str, price: Decimal) -> None:
    prev = last_prices.get(symbol)
    if prev and prev > 0:
        change_pct = abs((price - prev) / prev) * 100
        if change_pct > 5:
            event = DataQualityEvent(
                symbol=symbol,
                event_type="price_spike",
                severity="warning",
                message=f"Price moved {change_pct:.2f}% since last tick",
                time=datetime.now(timezone.utc),
                metadata={"previous": str(prev), "current": str(price)},
            )
            publish(producer, DATA_QUALITY, symbol, event)
            DATA_QUALITY_EVENTS.labels(event_type="price_spike", severity="warning").inc()
    last_prices[symbol] = price


def handle_message(topic: str, data: dict) -> None:
    start = time.perf_counter()
    symbol = data.get("symbol", "")

    if topic == RAW_TRADES:
        price = Decimal(str(data["price"]))
        validate_price(symbol, price)
        asyncio.run(persist_trade(data))

        l1 = cache.get_l1(symbol) or {}
        l1.update({"last_price": str(price), "last_size": str(data.get("size")), "time": data["time"]})
        cache.set_l1(symbol, l1)
        cache.publish_realtime(f"realtime:{symbol}", {"type": "trade", **data})

        snapshot = Level1Snapshot(
            symbol=symbol,
            exchange=data["exchange"],
            time=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            last_price=price,
            last_size=Decimal(str(data["size"])),
            bid_price=Decimal(str(l1["bid_price"])) if l1.get("bid_price") else None,
            ask_price=Decimal(str(l1["ask_price"])) if l1.get("ask_price") else None,
        )
        publish(producer, NORMALIZED_L1, symbol, snapshot)

    elif topic == RAW_QUOTES:
        asyncio.run(persist_quote(data))

        l1 = cache.get_l1(symbol) or {}
        l1.update(
            {
                "bid_price": str(data.get("bid_price")),
                "bid_size": str(data.get("bid_size")),
                "ask_price": str(data.get("ask_price")),
                "ask_size": str(data.get("ask_size")),
                "time": data["time"],
            }
        )
        cache.set_l1(symbol, l1)
        cache.publish_realtime(f"realtime:{symbol}", {"type": "quote", **data})

        # Synthetic L2 from top-of-book (real systems would ingest full depth)
        l2 = Level2Snapshot(
            symbol=symbol,
            exchange=data["exchange"],
            time=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
            bids=[OrderBookLevel(price=Decimal(str(data["bid_price"])), size=Decimal(str(data["bid_size"])), level=1)]
            if data.get("bid_price")
            else [],
            asks=[OrderBookLevel(price=Decimal(str(data["ask_price"])), size=Decimal(str(data["ask_size"])), level=1)]
            if data.get("ask_price")
            else [],
        )
        publish(producer, NORMALIZED_L2, symbol, l2)

    MESSAGES_PROCESSED.labels(service="stream_processor", topic=topic, symbol=symbol).inc()
    PROCESSING_LATENCY.labels(service="stream_processor", topic=topic).observe(time.perf_counter() - start)
    producer.flush()


def run() -> None:
    start_metrics_server(settings.stream_processor_metrics_port)
    consumer = create_consumer(
        settings.kafka_bootstrap_servers,
        group_id="stream-processor",
        topics=[RAW_TRADES, RAW_QUOTES],
    )
    logger.info("Stream processor listening on %s, %s", RAW_TRADES, RAW_QUOTES)
    consume_loop(consumer, handle_message)


if __name__ == "__main__":
    run()
