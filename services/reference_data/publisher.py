"""Reference data publisher — symbols, calendars, corporate actions."""

import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "libs" / "mds_common"))

from mds_common.config import settings
from mds_common.kafka.client import create_producer, publish
from mds_common.schemas.events import AssetClass, CorporateAction, ReferenceSymbol
from mds_common.topics import CORPORATE_ACTIONS, REFERENCE_SYMBOLS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

REFERENCE_SYMBOLS_DATA = [
    ReferenceSymbol(symbol="AAPL", exchange="NASDAQ", isin="US0378331005"),
    ReferenceSymbol(symbol="MSFT", exchange="NASDAQ", isin="US5949181045"),
    ReferenceSymbol(symbol="GOOGL", exchange="NASDAQ", isin="US02079K3059"),
    ReferenceSymbol(symbol="AMZN", exchange="NASDAQ", isin="US0231351067"),
    ReferenceSymbol(symbol="TSLA", exchange="NASDAQ", isin="US88160R1014"),
    ReferenceSymbol(symbol="SPX", exchange="CBOE", asset_class=AssetClass.INDEX),
]

SAMPLE_CORPORATE_ACTIONS = [
    CorporateAction(
        symbol="AAPL",
        action_type="dividend",
        ex_date=datetime(2025, 8, 11, tzinfo=timezone.utc),
        amount=Decimal("0.25"),
        currency="USD",
        description="Quarterly cash dividend",
    ),
]


def run() -> None:
    producer = create_producer(settings.kafka_bootstrap_servers)

    for sym in REFERENCE_SYMBOLS_DATA:
        publish(producer, REFERENCE_SYMBOLS, sym.symbol, sym)
        logger.info("Published reference data for %s", sym.symbol)

    for action in SAMPLE_CORPORATE_ACTIONS:
        publish(producer, CORPORATE_ACTIONS, action.symbol, action)
        logger.info("Published corporate action for %s: %s", action.symbol, action.action_type)

    producer.flush()
    logger.info("Reference data publish complete")


if __name__ == "__main__":
    run()
