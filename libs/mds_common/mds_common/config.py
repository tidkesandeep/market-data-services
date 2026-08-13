"""Shared configuration for all MDS services."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+asyncpg://mds:mds_secret@localhost:5432/market_data"

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_key: str = "dev-api-key-change-in-production"

    ingestion_symbols: str = "AAPL,MSFT,GOOGL,AMZN,TSLA"
    ingestion_source: str = "simulator"  # simulator | polygon
    polygon_api_key: str = ""
    simulator_tick_interval_ms: int = 100

    delayed_feed_minutes: int = 15
    anomaly_zscore_threshold: float = 3.0
    anomaly_window_size: int = 100

    # Reconciliation
    reconciliation_interval_seconds: int = 300
    min_trades_per_hour: int = 10
    min_quotes_per_hour: int = 10

    # Metrics ports (Prometheus scrape targets)
    api_metrics_port: int = 8000
    ingestion_metrics_port: int = 9101
    stream_processor_metrics_port: int = 9102
    reconciliation_metrics_port: int = 9103
    ai_analytics_metrics_port: int = 9104

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().upper() for s in self.ingestion_symbols.split(",") if s.strip()]


settings = Settings()
