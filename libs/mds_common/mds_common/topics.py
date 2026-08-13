"""Canonical Kafka topic names for the MDS platform."""

RAW_TRADES = "raw.trades"
RAW_QUOTES = "raw.quotes"
RAW_ORDERBOOK = "raw.orderbook"
RAW_INDEX = "raw.index"

NORMALIZED_TRADES = "normalized.trades"
NORMALIZED_QUOTES = "normalized.quotes"
NORMALIZED_L1 = "normalized.l1"
NORMALIZED_L2 = "normalized.l2"
NORMALIZED_INDEX = "normalized.index"

DELAYED_PRICES = "delayed.prices"
CORPORATE_ACTIONS = "corporate.actions"
REFERENCE_SYMBOLS = "reference.symbols"

DATA_QUALITY = "data.quality"
ANOMALIES = "analytics.anomalies"

ALL_TOPICS = [
    RAW_TRADES,
    RAW_QUOTES,
    RAW_ORDERBOOK,
    RAW_INDEX,
    NORMALIZED_TRADES,
    NORMALIZED_QUOTES,
    NORMALIZED_L1,
    NORMALIZED_L2,
    NORMALIZED_INDEX,
    DELAYED_PRICES,
    CORPORATE_ACTIONS,
    REFERENCE_SYMBOLS,
    DATA_QUALITY,
    ANOMALIES,
]
