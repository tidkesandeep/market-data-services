"""AI analytics — rolling z-score anomaly detection on trade prices."""

import asyncio
import logging
import sys
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import mean, stdev

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings
from mds_common.kafka.client import consume_loop, create_consumer, create_producer, publish
from mds_common.metrics import ANOMALIES_DETECTED, start_metrics_server
from mds_common.schemas.events import AnomalyEvent
from mds_common.topics import ANOMALIES, RAW_TRADES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
producer = create_producer(settings.kafka_bootstrap_servers)

windows: dict[str, deque[float]] = {}
threshold = settings.anomaly_zscore_threshold
window_size = settings.anomaly_window_size


async def persist_anomaly(event: AnomalyEvent) -> None:
    async with SessionLocal() as session:
        await session.execute(
            text(
                """
                INSERT INTO anomalies (time, symbol, metric, observed_value, expected_value, z_score, model_version)
                VALUES (:time, :symbol, :metric, :observed_value, :expected_value, :z_score, :model_version)
                """
            ),
            {
                "time": event.time,
                "symbol": event.symbol,
                "metric": event.metric,
                "observed_value": float(event.observed_value),
                "expected_value": float(event.expected_value) if event.expected_value else None,
                "z_score": float(event.z_score) if event.z_score else None,
                "model_version": event.model_version,
            },
        )
        await session.commit()


def detect_anomaly(symbol: str, price: float) -> AnomalyEvent | None:
    if symbol not in windows:
        windows[symbol] = deque(maxlen=window_size)

    window = windows[symbol]
    window.append(price)

    if len(window) < 20:
        return None

    mu = mean(window)
    sigma = stdev(window) if len(window) > 1 else 0.0
    if sigma == 0:
        return None

    z = (price - mu) / sigma
    if abs(z) >= threshold:
        return AnomalyEvent(
            symbol=symbol,
            metric="trade_price",
            observed_value=Decimal(str(round(price, 4))),
            expected_value=Decimal(str(round(mu, 4))),
            z_score=Decimal(str(round(z, 4))),
            time=datetime.now(timezone.utc),
        )
    return None


def handle_message(topic: str, data: dict) -> None:
    if topic != RAW_TRADES:
        return

    symbol = data["symbol"]
    price = float(data["price"])
    anomaly = detect_anomaly(symbol, price)

    if anomaly:
        logger.warning("Anomaly detected: %s price=%s z=%s", symbol, price, anomaly.z_score)
        publish(producer, ANOMALIES, symbol, anomaly)
        ANOMALIES_DETECTED.labels(symbol=symbol).inc()
        asyncio.run(persist_anomaly(anomaly))

    producer.flush()


def run() -> None:
    start_metrics_server(settings.ai_analytics_metrics_port)
    consumer = create_consumer(
        settings.kafka_bootstrap_servers,
        group_id="ai-analytics",
        topics=[RAW_TRADES],
    )
    logger.info("AI analytics listening on %s (z-score threshold=%s)", RAW_TRADES, threshold)
    consume_loop(consumer, handle_message)


if __name__ == "__main__":
    run()
