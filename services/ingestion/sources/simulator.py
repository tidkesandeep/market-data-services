"""Simulated market data source for local development."""

import logging
import random
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable

from confluent_kafka import Producer

from mds_common.config import settings
from mds_common.kafka.client import publish
from mds_common.metrics import MESSAGES_PUBLISHED
from mds_common.schemas.events import IndexValue, QuoteEvent, TradeEvent
from mds_common.topics import RAW_INDEX, RAW_QUOTES, RAW_TRADES

from .base import MarketDataSource

logger = logging.getLogger(__name__)

BASE_PRICES: dict[str, float] = {
    "AAPL": 190.0,
    "MSFT": 420.0,
    "GOOGL": 175.0,
    "AMZN": 185.0,
    "TSLA": 250.0,
    "SPX": 5200.0,
}


class SimulatorSource(MarketDataSource):
    def run(self, producer: Producer, symbols: list[str], on_event: Callable[[str, str, object], None]) -> None:
        prices = {s: BASE_PRICES.get(s, 100.0) for s in symbols}
        if "SPX" not in prices:
            prices["SPX"] = BASE_PRICES["SPX"]

        logger.info("Simulator running for: %s", list(prices.keys()))

        try:
            while True:
                for symbol, price in list(prices.items()):
                    trade, quote, new_price = self._simulate_tick(symbol, price)
                    prices[symbol] = new_price

                    publish(producer, RAW_TRADES, symbol, trade)
                    MESSAGES_PUBLISHED.labels(service="ingestion", topic=RAW_TRADES, symbol=symbol).inc()
                    on_event(producer, RAW_TRADES, trade)

                    publish(producer, RAW_QUOTES, symbol, quote)
                    MESSAGES_PUBLISHED.labels(service="ingestion", topic=RAW_QUOTES, symbol=symbol).inc()
                    on_event(producer, RAW_QUOTES, quote)

                    if symbol == "SPX":
                        index = IndexValue(
                            index_symbol="SPX",
                            value=trade.price,
                            time=trade.time,
                            change_pct=Decimal(str(round(random.uniform(-0.5, 0.5), 4))),
                        )
                        publish(producer, RAW_INDEX, "SPX", index)
                        MESSAGES_PUBLISHED.labels(service="ingestion", topic=RAW_INDEX, symbol="SPX").inc()

                producer.flush()
                time.sleep(settings.simulator_tick_interval_ms / 1000)
        except KeyboardInterrupt:
            logger.info("Simulator stopped")
            producer.flush()

    @staticmethod
    def _simulate_tick(symbol: str, price: float) -> tuple[TradeEvent, QuoteEvent, float]:
        now = datetime.now(timezone.utc)
        delta = random.uniform(-0.5, 0.5)
        new_price = max(0.01, price + delta)
        spread = random.uniform(0.01, 0.05)

        trade = TradeEvent(
            symbol=symbol,
            exchange="NASDAQ" if symbol != "SPX" else "CBOE",
            price=Decimal(str(round(new_price, 4))),
            size=Decimal(str(random.randint(1, 500))),
            time=now,
            trade_id=f"{symbol}-{int(now.timestamp() * 1000)}",
            side=random.choice(["buy", "sell", "unknown"]),
            source="simulator",
        )

        quote = QuoteEvent(
            symbol=symbol,
            exchange=trade.exchange,
            bid_price=Decimal(str(round(new_price - spread / 2, 4))),
            bid_size=Decimal(str(random.randint(100, 1000))),
            ask_price=Decimal(str(round(new_price + spread / 2, 4))),
            ask_size=Decimal(str(random.randint(100, 1000))),
            time=now,
            source="simulator",
        )
        return trade, quote, new_price
