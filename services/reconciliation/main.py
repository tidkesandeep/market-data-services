"""Reconciliation service — compares tick volumes against expected thresholds."""

import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings
from mds_common.kafka.client import create_producer, publish
from mds_common.metrics import DATA_QUALITY_EVENTS, RECONCILIATION_GAPS, start_metrics_server
from mds_common.schemas.events import DataQualityEvent
from mds_common.topics import DATA_QUALITY

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
producer = create_producer(settings.kafka_bootstrap_servers)


async def count_records(table: str, symbol: str, since: datetime) -> int:
    async with SessionLocal() as session:
        result = await session.execute(
            text(f"SELECT COUNT(*) AS cnt FROM {table} WHERE symbol = :symbol AND time >= :since"),
            {"symbol": symbol, "since": since},
        )
        row = result.mappings().one()
    return int(row["cnt"])


async def persist_reconciliation(symbol: str, metric: str, observed: int, expected: int, gap: int) -> None:
    async with SessionLocal() as session:
        await session.execute(
            text(
                """
                INSERT INTO reconciliation_runs
                    (time, symbol, metric, observed_count, expected_min, gap, status)
                VALUES (:time, :symbol, :metric, :observed, :expected, :gap, :status)
                """
            ),
            {
                "time": datetime.now(timezone.utc),
                "symbol": symbol,
                "metric": metric,
                "observed": observed,
                "expected": expected,
                "gap": gap,
                "status": "fail" if gap < 0 else "pass",
            },
        )
        await session.commit()


async def reconcile_symbol(symbol: str, since: datetime) -> None:
    trade_count = await count_records("trades", symbol, since)
    quote_count = await count_records("quotes", symbol, since)

    checks = [
        ("trades", trade_count, settings.min_trades_per_hour),
        ("quotes", quote_count, settings.min_quotes_per_hour),
    ]

    for metric, observed, expected_min in checks:
        gap = observed - expected_min
        RECONCILIATION_GAPS.labels(symbol=symbol, metric=metric).set(gap)

        await persist_reconciliation(symbol, metric, observed, expected_min, gap)

        if gap < 0:
            event = DataQualityEvent(
                symbol=symbol,
                event_type="reconciliation_gap",
                severity="error",
                message=f"{metric} count {observed} below minimum {expected_min} in last hour",
                time=datetime.now(timezone.utc),
                metadata={"observed": observed, "expected_min": expected_min, "gap": gap},
            )
            publish(producer, DATA_QUALITY, symbol, event)
            DATA_QUALITY_EVENTS.labels(event_type="reconciliation_gap", severity="error").inc()
            logger.warning("Reconciliation FAIL %s %s: observed=%d expected_min=%d", symbol, metric, observed, expected_min)
        else:
            logger.info("Reconciliation PASS %s %s: observed=%d expected_min=%d", symbol, metric, observed, expected_min)

    producer.flush()


async def run_cycle() -> None:
    since = datetime.now(timezone.utc) - timedelta(hours=1)
    for symbol in settings.symbol_list:
        await reconcile_symbol(symbol, since)


def run() -> None:
    start_metrics_server(settings.reconciliation_metrics_port)
    logger.info(
        "Reconciliation service started (interval=%ds, min_trades=%d, min_quotes=%d)",
        settings.reconciliation_interval_seconds,
        settings.min_trades_per_hour,
        settings.min_quotes_per_hour,
    )

    try:
        while True:
            asyncio.run(run_cycle())
            time.sleep(settings.reconciliation_interval_seconds)
    except KeyboardInterrupt:
        logger.info("Reconciliation service stopped")


if __name__ == "__main__":
    run()
