# Market Data Services (MDS)

A high-throughput platform for collecting, processing, enriching, and distributing financial market data — built for data engineers and AI engineers.

## Products

- **Real-time feed** — live prices, trades, quotes via WebSocket
- **Delayed feed** — prices delayed 15–20 minutes
- **Historical database** — years of trades and quotes in TimescaleDB
- **Tick data** — every individual market event
- **Level 1** — best bid, best ask, last traded price
- **Level 2** — order book depth
- **Index feed** — live index values
- **Corporate actions** — dividends, splits, mergers
- **Reference data** — identifiers, calendars, symbol mappings

## Stack

| Layer | Technology |
|-------|------------|
| Streaming | Redpanda (Kafka-compatible) |
| Cache / pub-sub | Redis |
| Time-series DB | TimescaleDB |
| API | FastAPI + WebSocket |
| Language | Python 3.11+ |
| AI | Rolling z-score anomaly detection (extensible) |

## Quick Start

### Prerequisites

- Docker Desktop
- Python 3.11+

### 1. Start infrastructure

```bash
docker compose up -d
```

Services: Redpanda (`9092`), Redis (`6379`), TimescaleDB (`5432`), Redpanda Console (`8080`).

### 2. Install dependencies

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -e .
```

### 3. Configure environment

```bash
copy .env.example .env
```

### 4. Create Kafka topics

```bash
python scripts/create_topics.py
```

### 5. Run services (separate terminals)

```bash
python services/ingestion/simulator.py
python services/stream_processor/main.py
python services/delayed_feed/main.py
python services/ai_analytics/anomaly_detector.py
python services/api/main.py
```

Optional: publish reference data once:

```bash
python services/reference_data/publisher.py
```

### 6. Query the API

```bash
curl -H "X-API-Key: dev-api-key-change-in-production" http://localhost:8000/health
curl -H "X-API-Key: dev-api-key-change-in-production" http://localhost:8000/v1/l1/AAPL
curl -H "X-API-Key: dev-api-key-change-in-production" "http://localhost:8000/v1/trades/AAPL?limit=10"
```

WebSocket (real-time stream):

```
ws://localhost:8000/ws/v1/stream/AAPL
```

## Project Structure

```
market-data-services/
├── docker-compose.yml          # Redpanda, Redis, TimescaleDB
├── infra/timescaledb/init.sql    # Schema + seed data
├── libs/mds_common/              # Shared schemas, Kafka/Redis clients
├── services/
│   ├── ingestion/                # Market simulator (replace with vendor connectors)
│   ├── stream_processor/         # Normalize, validate, persist, cache
│   ├── delayed_feed/             # 15-min delayed prices
│   ├── reference_data/           # Symbols, corporate actions
│   ├── ai_analytics/             # Anomaly detection
│   └── api/                      # REST + WebSocket gateway
├── scripts/create_topics.py
└── docs/architecture.md
```

## Your Learning Path

| Focus | Where to start |
|-------|----------------|
| Stream processing | `services/stream_processor/main.py` |
| Low-latency messaging | `libs/mds_common/redis/cache.py` + WebSocket in API |
| Data quality | Price spike validation in stream processor |
| Time-series storage | `infra/timescaledb/init.sql` |
| API development | `services/api/main.py` |
| AI / anomaly detection | `services/ai_analytics/anomaly_detector.py` |
| Distributed systems | Kafka consumer groups across services |

See [docs/architecture.md](docs/architecture.md) for the full system design and roadmap.

## License

MIT
