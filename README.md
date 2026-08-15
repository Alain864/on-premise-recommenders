# On-Premise Recommenders

Search and recommendation system for an e-commerce catalog. Postgres is the source of truth (including pgvector embeddings). Elasticsearch is used for BM25 only. Redis is the Celery broker.

The ranking algorithms are unchanged from the original prototype: category affinity, co-purchase / co-view counts, and a weighted BM25 re-rank.

## Stack

- API: FastAPI
- Database: PostgreSQL 16 + pgvector
- Search: Elasticsearch 8 (lexical)
- Jobs: Celery + Redis
- Frontend: Vite + React + TypeScript

## Prerequisites

- Python 3.12+
- Docker
- Node 18+ (frontend)
- OpenAI API key (embeddings / semantic fallback)

## Quick start

### 1. Start infrastructure

```bash
docker compose up -d
```

Wait until Postgres, Elasticsearch, and Redis are healthy.

### 2. Install and configure

```bash
python3 -m venv .venv
./.venv/bin/pip install -e ".[dev]"
cp .env.example .env
# Set OPENAI_API_KEY in .env
```

`DATABASE_URL` must be PostgreSQL (`postgresql+psycopg://...`). SQLite is not supported.

### 3. Load data and build features

```bash
./.venv/bin/recommender run-pipeline --source-dir ./data/parquet
```

This runs Alembic migrations, loads parquet files, rebuilds derived tables in SQL, writes query suggestions and trending products, indexes Elasticsearch, and generates missing embeddings.

Skip external steps when iterating on tables only:

```bash
./.venv/bin/recommender run-pipeline --source-dir ./data/parquet --skip-search --skip-embeddings
```

### 4. Run the API

```bash
./.venv/bin/uvicorn onprem_recommenders.app:app --reload
```

Health check: `http://127.0.0.1:8000/health`

### 5. Run the frontend

```bash
cd frontend
cp .env.example .env
npm install
npm run dev
```

Open `http://localhost:5173`. Use the **User ID** field in the nav to demo personalization (for example `USR_13914CAFA179` after loading the sample parquet).

## Pipeline commands

```bash
./.venv/bin/recommender init-db
./.venv/bin/recommender load-parquet --source-dir ./data/parquet
./.venv/bin/recommender build-derived
./.venv/bin/recommender build-autocomplete
./.venv/bin/recommender infra compute-trending
./.venv/bin/recommender sync-search
./.venv/bin/recommender sync-embeddings
```

Nightly jobs (derived tables, trending, suggestions) can be run in-process:

```bash
./.venv/bin/recommender infra run-nightly
```

Or with Celery:

```bash
./.venv/bin/celery -A onprem_recommenders.workers.celery_app worker --loglevel=info
./.venv/bin/celery -A onprem_recommenders.workers.celery_app beat --loglevel=info
```

`run_all_nightly` enqueues the child tasks with `.delay()`; it does not run them in the beat process.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /recommendations/homepage` | Affinity rows, or trending from `trending_products` |
| `GET /recommendations/search` | BM25 + weighted re-rank; pgvector fallback when BM25 is weak |
| `GET /recommendations/product/{id}` | Product detail |
| `GET /recommendations/product/{id}/frequently-bought-together` | Co-purchase, category fallback |
| `GET /recommendations/product/{id}/customers-also-viewed` | Co-view, category fallback |
| `GET /autocomplete/suggest` | In-memory prefix index |
| `POST /events` | Click / add-to-cart logging |
| `GET /feature-flags` | List flags |
| `PUT /feature-flags/{name}` | Enable/disable a feature |
| `GET /trending` | Precomputed trending rows |

Impression events are written in a FastAPI background task on homepage, search, product recs, and autocomplete responses. Feature flags (`homepage`, `search_ranking`, `product_page`, `autocomplete`) are read on those paths; a disabled flag forces the fallback behavior.

Search ranking weights: BM25 40%, popularity 25%, conversion 15%, review 10%, in-stock 10%. Review and stock are still prototype defaults until those feeds exist.

## Tests

```bash
./.venv/bin/pytest tests/test_ranking.py tests/test_autocomplete.py
```

API tests require a loaded database and Elasticsearch:

```bash
RUN_API_TESTS=1 ./.venv/bin/pytest tests/test_api.py
```

## Environment

| Variable | Default |
| --- | --- |
| `DATABASE_URL` | `postgresql+psycopg://recommender:recommender@localhost:5432/recommender` |
| `ELASTICSEARCH_URL` | `http://localhost:9200` |
| `ELASTICSEARCH_INDEX` | `products` |
| `REDIS_URL` | `redis://localhost:6379/0` |
| `OPENAI_API_KEY` | required for embeddings |
| `OPENAI_EMBEDDING_MODEL` | `text-embedding-3-small` |
| `SOURCE_DATA_DIR` | `./data/parquet` |
| `CORS_ORIGINS` | `http://localhost:5173,http://127.0.0.1:5173` |
| `VITE_API_URL` | `http://127.0.0.1:8000` |
