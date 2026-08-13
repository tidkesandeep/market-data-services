"""Abstract market data source interface."""

from abc import ABC, abstractmethod
from typing import Callable

from confluent_kafka import Producer

from mds_common.schemas.events import QuoteEvent, TradeEvent


class MarketDataSource(ABC):
    @abstractmethod
    def run(self, producer: Producer, symbols: list[str], on_event: Callable[[str, str, object], None]) -> None:
        """Stream market data. Calls on_event(producer, topic, event) for each tick."""
        ...
