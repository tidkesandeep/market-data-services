"""Ingestion entry point — selects market data source via INGESTION_SOURCE."""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))
sys.path.insert(0, str(ROOT / "services" / "ingestion"))

from mds_common.config import settings
from mds_common.kafka.client import create_producer
from mds_common.metrics import ACTIVE_SYMBOLS, start_metrics_server

from sources.polygon import PolygonSource
from sources.simulator import SimulatorSource

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _noop(_producer: object, _topic: str, _event: object) -> None:
    pass


def get_source() -> SimulatorSource | PolygonSource:
    if settings.ingestion_source == "polygon":
        if not settings.polygon_api_key:
            raise ValueError("POLYGON_API_KEY is required when INGESTION_SOURCE=polygon")
        return PolygonSource(api_key=settings.polygon_api_key)
    return SimulatorSource()


def run() -> None:
    start_metrics_server(settings.ingestion_metrics_port)
    producer = create_producer(settings.kafka_bootstrap_servers)
    source = get_source()
    symbols = settings.symbol_list

    ACTIVE_SYMBOLS.labels(service="ingestion").set(len(symbols))
    logger.info("Starting ingestion source=%s symbols=%s", settings.ingestion_source, symbols)

    source.run(producer, symbols, _noop)


if __name__ == "__main__":
    run()
