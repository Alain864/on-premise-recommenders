# On-Premise Recommenders

A proof-of-concept search and recommendation system for an e-commerce platform.

## Stage 1: Data Foundation

Stage 1 implements the data foundation for the search and recommendation prototype:

- Ingest source data from four parquet files: `users.parquet`, `products.parquet`, `transactions.parquet`, and `interactions.parquet`
- Materialize source and derived tables in SQL
- Compute `user_category_affinity`, `product_stats`, `co_purchase_pairs`, and `co_view_pairs`
- Sync products into Elasticsearch
- Generate OpenAI embeddings and persist them in ChromaDB

## Stage 2: Personalized Homepage Feed API

Stage 2 implements the personalized homepage recommendations endpoint:

- **Anonymous users**: Returns trending products by category
- **Known users with affinity data**: Returns personalized recommendations from user's top affinity categories, excluding already-purchased products
- **Cold-start users (no affinity data)**: Falls back to trending products

## Prerequisites

- Python 3.12+
- Docker (for Elasticsearch)
- OpenAI API key

## Quick start

### 1. Install dependencies

```bash
./.venv/bin/pip install setuptools wheel
./.venv/bin/pip install -e . --no-build-isolation
```

### 2. Start Elasticsearch

```bash
docker-compose up -d
```

Wait for Elasticsearch to be ready (usually 30-60 seconds):

```bash
curl -s http://localhost:9200/_cluster/health | jq .status
# Should return "green" or "yellow"
```

### 3. Configure environment

Create a `.env` file with your OpenAI API key:

```bash
cp .env.example .env
# Edit .env and add your OpenAI API key
```

Required settings:

```
OPENAI_API_KEY=sk-your-api-key-here
ELASTICSEARCH_URL=http://localhost:9200
```

### 4. Run the Stage 1 pipeline

```bash
./.venv/bin/recommender-stage1 run-stage1 --source-dir ./data/parquet
```

This creates a local prototype database at `var/stage1.db`, syncs products to Elasticsearch, and generates embeddings in ChromaDB.

## Environment variables

All configuration is handled via environment variables (loaded from `.env` file):

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Database connection string | `sqlite+pysqlite:///./var/stage1.db` |
| `ELASTICSEARCH_URL` | Elasticsearch endpoint | `http://localhost:9200` |
| `ELASTICSEARCH_INDEX` | Products index name | `products` |
| `CHROMA_PERSIST_DIRECTORY` | ChromaDB storage path | `./var/chroma` |
| `CHROMA_COLLECTION` | Embedding collection name | `product_embeddings` |
| `OPENAI_API_KEY` | OpenAI API key (required for embeddings) | - |
| `OPENAI_EMBEDDING_MODEL` | OpenAI embedding model | `text-embedding-3-small` |
| `SOURCE_DATA_DIR` | Parquet files directory | `./data/parquet` |
| `SESSION_GAP_MINUTES` | Session gap threshold | `30` |
| `EMBEDDING_BATCH_SIZE` | OpenAI batch size | `50` |

## CLI commands

Initialize the schema only:

```bash
./.venv/bin/recommender-stage1 init-db
```

Load the parquet files into the source tables:

```bash
./.venv/bin/recommender-stage1 load-parquet --source-dir ./data/parquet
```

Build derived tables:

```bash
./.venv/bin/recommender-stage1 build-derived
```

Sync external stores (requires Elasticsearch and OpenAI API key):

```bash
./.venv/bin/recommender-stage1 sync-search
./.venv/bin/recommender-stage1 sync-embeddings
```

Run full Stage 1 pipeline:

```bash
./.venv/bin/recommender-stage1 run-stage1 --source-dir ./data/parquet
```

## Development without external services

For local development without Elasticsearch or OpenAI:

```bash
# Database and derived tables only
./.venv/bin/recommender-stage1 run-stage1 --source-dir ./data/parquet --skip-search --skip-embeddings
```

Note: Skipping services limits functionality:
- Without Elasticsearch: full-text search and autocomplete unavailable
- Without OpenAI: semantic search unavailable

## Running the API Server

Start the FastAPI server:

```bash
./.venv/bin/uvicorn onprem_recommenders.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`.

### API Endpoints

#### Homepage Recommendations

```
GET /recommendations/homepage
```

Query parameters:
- `user_id` (optional): User ID for personalization. If omitted, returns trending products.
- `rows` (default=3, range 1-10): Number of recommendation rows
- `products_per_row` (default=10, range 1-50): Products per row

Example requests:

```bash
# Personalized recommendations for a known user
curl -s "http://127.0.0.1:8000/recommendations/homepage?user_id=USR_13914CAFA179&rows=2&products_per_row=5" | jq .

# Anonymous user (trending products)
curl -s "http://127.0.0.1:8000/recommendations/homepage?rows=2&products_per_row=5" | jq .

# Find valid user IDs with affinity data
sqlite3 var/stage1.db "SELECT DISTINCT user_id FROM user_category_affinity LIMIT 10;"
```

Response format:

```json
{
  "user_id": "USR_13914CAFA179",
  "rows": [
    {
      "row_label": "Recommended in AC Adapters (Electronics)",
      "products": [
        {
          "product_id": "B08KXZXCL6",
          "title": "Product title...",
          "brand": "Brand name",
          "price": 14.99,
          "category_path": "Electronics > Computers & Accessories > ...",
          "popularity_score": 3.93
        }
      ]
    }
  ],
  "is_personalized": true
}
```

## Docker services

The `docker-compose.yml` provides:

- **Elasticsearch** (port 9200) - Full-text search index

Start services:

```bash
docker-compose up -d
```

Stop services:

```bash
docker-compose down
```

Stop and remove volumes:

```bash
docker-compose down -v
```

## Notes

- The sample data does not include review or stock feeds yet, so `product_stats.review_score`, `product_stats.review_count`, and `product_stats.in_stock` use sensible prototype defaults.
- The code is PostgreSQL-ready, but defaults to SQLite so the sample pipeline can run immediately in a fresh local environment.

## Parquet file format

The `products.parquet` file should contain a `category_path` column. This column can be either:

- A **list or numpy array** of category strings (e.g., `['Electronics', 'Computers & Accessories', 'Monitors']`) — this is the preferred format and will be automatically converted to a delimited string.
- A **string** with categories separated by ` > ` (e.g., `"Electronics > Computers & Accessories > Monitors"`).

The loader will automatically convert array/list values to the `" > "`-delimited string format expected by the database schema (`String(512)`). The resulting string length must not exceed 512 characters.