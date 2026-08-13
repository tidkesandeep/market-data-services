"""FastAPI gateway — REST + WebSocket for real-time and historical market data."""

import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Market Data Services API",
    description="Real-time, delayed, and historical market data distribution",
    version="0.1.0",
)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
engine = create_async_engine(settings.database_url, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def verify_api_key(key: str | None = Depends(api_key_header)) -> None:
    if key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


class HealthResponse(BaseModel):
    status: str
    redis: bool
    database: bool


class SymbolResponse(BaseModel):
    symbol: str
    exchange: str
    asset_class: str
    currency: str
    active: bool


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    redis_ok = False
    db_ok = False
    try:
        r = aioredis.from_url(settings.redis_url)
        redis_ok = await r.ping()
        await r.aclose()
    except Exception:
        pass
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
            db_ok = True
    except Exception:
        pass
    return HealthResponse(
        status="healthy" if redis_ok and db_ok else "degraded",
        redis=redis_ok,
        database=db_ok,
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/symbols", response_model=list[SymbolResponse], dependencies=[Depends(verify_api_key)])
async def list_symbols() -> list[SymbolResponse]:
    async with SessionLocal() as session:
        result = await session.execute(
            text("SELECT symbol, exchange, asset_class, currency, active FROM symbols WHERE active = TRUE ORDER BY symbol")
        )
        rows = result.mappings().all()
    return [SymbolResponse(**row) for row in rows]


@app.get("/v1/l1/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_l1(symbol: str) -> dict[str, Any]:
    r = aioredis.from_url(settings.redis_url)
    raw = await r.get(f"l1:{symbol.upper()}")
    await r.aclose()
    if not raw:
        raise HTTPException(status_code=404, detail=f"No L1 data for {symbol}")
    return json.loads(raw)


@app.get("/v1/trades/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_trades(
    symbol: str,
    limit: int = Query(default=100, le=1000),
    hours: int = Query(default=24, le=168),
) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with SessionLocal() as session:
        result = await session.execute(
            text(
                """
                SELECT time, symbol, exchange, price, size, trade_id, side
                FROM trades
                WHERE symbol = :symbol AND time >= :since
                ORDER BY time DESC
                LIMIT :limit
                """
            ),
            {"symbol": symbol.upper(), "since": since, "limit": limit},
        )
        rows = result.mappings().all()
    return [dict(row) for row in rows]


@app.get("/v1/quotes/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_quotes(
    symbol: str,
    limit: int = Query(default=100, le=1000),
    hours: int = Query(default=24, le=168),
) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with SessionLocal() as session:
        result = await session.execute(
            text(
                """
                SELECT time, symbol, exchange, bid_price, bid_size, ask_price, ask_size
                FROM quotes
                WHERE symbol = :symbol AND time >= :since
                ORDER BY time DESC
                LIMIT :limit
                """
            ),
            {"symbol": symbol.upper(), "since": since, "limit": limit},
        )
        rows = result.mappings().all()
    return [dict(row) for row in rows]


@app.get("/v1/anomalies/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_anomalies(
    symbol: str,
    limit: int = Query(default=50, le=500),
) -> list[dict[str, Any]]:
    async with SessionLocal() as session:
        result = await session.execute(
            text(
                """
                SELECT time, symbol, metric, observed_value, expected_value, z_score, model_version
                FROM anomalies
                WHERE symbol = :symbol
                ORDER BY time DESC
                LIMIT :limit
                """
            ),
            {"symbol": symbol.upper(), "limit": limit},
        )
        rows = result.mappings().all()
    return [dict(row) for row in rows]


@app.get("/v1/reconciliation/{symbol}", dependencies=[Depends(verify_api_key)])
async def get_reconciliation(
    symbol: str,
    limit: int = Query(default=20, le=200),
) -> list[dict[str, Any]]:
    async with SessionLocal() as session:
        result = await session.execute(
            text(
                """
                SELECT time, symbol, metric, observed_count, expected_min, gap, status
                FROM reconciliation_runs
                WHERE symbol = :symbol
                ORDER BY time DESC
                LIMIT :limit
                """
            ),
            {"symbol": symbol.upper(), "limit": limit},
        )
        rows = result.mappings().all()
    return [dict(row) for row in rows]


@app.websocket("/ws/v1/stream/{symbol}")
async def stream_symbol(websocket: WebSocket, symbol: str) -> None:
    await websocket.accept()
    r = aioredis.from_url(settings.redis_url)
    pubsub = r.pubsub()
    channel = f"realtime:{symbol.upper()}"
    await pubsub.subscribe(channel)
    logger.info("WebSocket client subscribed to %s", channel)

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                await websocket.send_text(message["data"].decode())
            await asyncio.sleep(0.01)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for %s", symbol)
    finally:
        await pubsub.unsubscribe(channel)
        await r.aclose()


def run() -> None:
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    run()
