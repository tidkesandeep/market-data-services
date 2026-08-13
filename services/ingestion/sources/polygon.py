"""Polygon.io WebSocket connector for live trades and quotes."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable

import websockets
from confluent_kafka import Producer

from mds_common.config import settings
from mds_common.kafka.client import publish
from mds_common.metrics import MESSAGES_PUBLISHED
from mds_common.schemas.events import QuoteEvent, TradeEvent
from mds_common.topics import RAW_QUOTES, RAW_TRADES

from .base import MarketDataSource

logger = logging.getLogger(__name__)

POLYGON_WS_URL = "wss://socket.polygon.io/stocks"

EXCHANGE_MAP = {
    1: "NYSE",
    2: "AMEX",
    3: "NASDAQ",
    4: "NASDAQ",
    11: "OTC",
}


class PolygonSource(MarketDataSource):
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def run(self, producer: Producer, symbols: list[str], on_event: Callable[[str, str, object], None]) -> None:
        asyncio.run(self._stream(producer, symbols, on_event))

    async def _stream(
        self,
        producer: Producer,
        symbols: list[str],
        on_event: Callable[[str, str, object], None],
    ) -> None:
        trade_subs = ",".join(f"T.{s}" for s in symbols)
        quote_subs = ",".join(f"Q.{s}" for s in symbols)
        params = f"{trade_subs},{quote_subs}"

        logger.info("Connecting to Polygon.io for symbols: %s", symbols)

        while True:
            try:
                async with websockets.connect(POLYGON_WS_URL) as ws:
                    await ws.send(json.dumps({"action": "auth", "params": self.api_key}))
                    auth_resp = json.loads(await ws.recv())
                    logger.info("Polygon auth response: %s", auth_resp)

                    await ws.send(json.dumps({"action": "subscribe", "params": params}))
                    logger.info("Subscribed to %s", params)

                    async for raw in ws:
                        for msg in self._parse_messages(raw):
                            event = self._to_event(msg)
                            if event is None:
                                continue

                            topic, payload = event
                            symbol = payload.symbol
                            publish(producer, topic, symbol, payload)
                            MESSAGES_PUBLISHED.labels(
                                service="ingestion", topic=topic, symbol=symbol
                            ).inc()
                            on_event(producer, topic, payload)
                            producer.poll(0)

            except websockets.ConnectionClosed as exc:
                logger.warning("Polygon connection closed: %s — reconnecting in 5s", exc)
                await asyncio.sleep(5)
            except Exception as exc:
                logger.error("Polygon error: %s — reconnecting in 10s", exc)
                await asyncio.sleep(10)

    @staticmethod
    def _parse_messages(raw: str | bytes) -> list[dict]:
        data = json.loads(raw)
        if isinstance(data, list):
            return data
        return [data]

    def _to_event(self, msg: dict) -> tuple[str, TradeEvent | QuoteEvent] | None:
        ev = msg.get("ev")
        if ev == "status":
            logger.debug("Polygon status: %s", msg.get("message"))
            return None

        if ev == "T":
            ts = datetime.fromtimestamp(msg["t"] / 1000, tz=timezone.utc)
            return RAW_TRADES, TradeEvent(
                symbol=msg["sym"],
                exchange=EXCHANGE_MAP.get(msg.get("x", 0), "UNKNOWN"),
                price=Decimal(str(msg["p"])),
                size=Decimal(str(msg["s"])),
                time=ts,
                trade_id=str(msg.get("i", "")),
                side="unknown",
                source="polygon",
            )

        if ev == "Q":
            ts = datetime.fromtimestamp(msg["t"] / 1000, tz=timezone.utc)
            return RAW_QUOTES, QuoteEvent(
                symbol=msg["sym"],
                exchange=EXCHANGE_MAP.get(msg.get("x", 0), "UNKNOWN"),
                bid_price=Decimal(str(msg["bp"])) if msg.get("bp") else None,
                bid_size=Decimal(str(msg["bs"])) if msg.get("bs") else None,
                ask_price=Decimal(str(msg["ap"])) if msg.get("ap") else None,
                ask_size=Decimal(str(msg["as"])) if msg.get("as") else None,
                time=ts,
                source="polygon",
            )

        return None
