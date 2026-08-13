"""Prometheus metrics shared across MDS services."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

MESSAGES_PROCESSED = Counter(
    "mds_messages_processed_total",
    "Total messages processed",
    ["service", "topic", "symbol"],
)

MESSAGES_PUBLISHED = Counter(
    "mds_messages_published_total",
    "Total messages published to Kafka",
    ["service", "topic", "symbol"],
)

PROCESSING_LATENCY = Histogram(
    "mds_processing_latency_seconds",
    "Message processing latency",
    ["service", "topic"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0),
)

ACTIVE_SYMBOLS = Gauge(
    "mds_active_symbols",
    "Number of symbols currently tracked",
    ["service"],
)

RECONCILIATION_GAPS = Gauge(
    "mds_reconciliation_gaps",
    "Tick count gap vs expected minimum (negative = deficit)",
    ["symbol", "metric"],
)

DATA_QUALITY_EVENTS = Counter(
    "mds_data_quality_events_total",
    "Data quality events emitted",
    ["event_type", "severity"],
)

ANOMALIES_DETECTED = Counter(
    "mds_anomalies_detected_total",
    "Anomalies detected by AI analytics",
    ["symbol"],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
