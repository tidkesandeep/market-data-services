"""Redis helpers for low-latency L1 cache and pub/sub."""

import json
import logging
from typing import Any

import redis

logger = logging.getLogger(__name__)


class RedisCache:
    def __init__(self, url: str) -> None:
        self._client = redis.from_url(url, decode_responses=True)

    def set_l1(self, symbol: str, data: dict[str, Any], ttl_seconds: int = 60) -> None:
        key = f"l1:{symbol.upper()}"
        self._client.setex(key, ttl_seconds, json.dumps(data, default=str))

    def get_l1(self, symbol: str) -> dict[str, Any] | None:
        raw = self._client.get(f"l1:{symbol.upper()}")
        return json.loads(raw) if raw else None

    def publish_realtime(self, channel: str, data: dict[str, Any]) -> None:
        self._client.publish(channel, json.dumps(data, default=str))

    def ping(self) -> bool:
        return bool(self._client.ping())
