"""Pydantic schemas for market data events."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


class AssetClass(StrEnum):
    EQUITY = "equity"
    INDEX = "index"
    FX = "fx"
    CRYPTO = "crypto"


class TradeEvent(BaseModel):
    symbol: str
    exchange: str
    price: Decimal
    size: Decimal
    time: datetime
    trade_id: str | None = None
    side: Side = Side.UNKNOWN
    source: str = "simulator"


class QuoteEvent(BaseModel):
    symbol: str
    exchange: str
    bid_price: Decimal | None = None
    bid_size: Decimal | None = None
    ask_price: Decimal | None = None
    ask_size: Decimal | None = None
    time: datetime
    source: str = "simulator"


class Level1Snapshot(BaseModel):
    """Best bid, best ask, last traded price."""

    symbol: str
    exchange: str
    time: datetime
    bid_price: Decimal | None = None
    bid_size: Decimal | None = None
    ask_price: Decimal | None = None
    ask_size: Decimal | None = None
    last_price: Decimal | None = None
    last_size: Decimal | None = None
    source: str = "simulator"


class OrderBookLevel(BaseModel):
    price: Decimal
    size: Decimal
    level: int


class Level2Snapshot(BaseModel):
    symbol: str
    exchange: str
    time: datetime
    bids: list[OrderBookLevel] = Field(default_factory=list)
    asks: list[OrderBookLevel] = Field(default_factory=list)
    source: str = "simulator"


class IndexValue(BaseModel):
    index_symbol: str
    value: Decimal
    time: datetime
    change_pct: Decimal | None = None
    source: str = "simulator"


class CorporateAction(BaseModel):
    symbol: str
    action_type: str
    ex_date: datetime
    record_date: datetime | None = None
    pay_date: datetime | None = None
    ratio: Decimal | None = None
    amount: Decimal | None = None
    currency: str | None = None
    description: str | None = None


class ReferenceSymbol(BaseModel):
    symbol: str
    exchange: str
    asset_class: AssetClass = AssetClass.EQUITY
    currency: str = "USD"
    isin: str | None = None
    cusip: str | None = None
    figi: str | None = None
    active: bool = True


class DelayedPrice(BaseModel):
    symbol: str
    exchange: str
    price: Decimal
    time: datetime
    original_time: datetime
    delay_minutes: int


class DataQualityEvent(BaseModel):
    symbol: str | None = None
    event_type: str
    severity: str
    message: str
    time: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)


class AnomalyEvent(BaseModel):
    symbol: str
    metric: str
    observed_value: Decimal
    expected_value: Decimal | None = None
    z_score: Decimal | None = None
    time: datetime
    model_version: str = "zscore-v1"
