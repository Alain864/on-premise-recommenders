# On-Premise Recommenders

Stage 1 implements the data foundation for the search and recommendation prototype:

- ingest source data from `sample_data.xlsx`
- materialize source and derived tables in SQL
- compute `user_category_affinity`, `product_stats`, `co_purchase_pairs`, and `co_view_pairs`
- sync products into Elasticsearch
- generate OpenAI embeddings and persist them in ChromaDB

## Quick start

Install the project in the virtualenv:

```bash
./.venv/bin/pip install setuptools wheel
./.venv/bin/pip install -e . --no-build-isolation
```

Run the Stage 1 pipeline locally with the sample workbook and a SQLite database:

```bash
./.venv/bin/recommender-stage1 run-stage1 --skip-search --skip-embeddings
```

That creates a local prototype database at `var/stage1.db`.

## Environment variables

Optional environment variables:

```bash
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/recommenders
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX=products
CHROMA_PERSIST_DIRECTORY=./var/chroma
CHROMA_COLLECTION=product_embeddings
OPENAI_EMBEDDING_MODEL=text-embedding-3-small
SOURCE_WORKBOOK=sample_data.xlsx
SESSION_GAP_MINUTES=30
```

## Useful commands

Initialize the schema only:

```bash
./.venv/bin/recommender-stage1 init-db
```

Load the workbook into the source tables:

```bash
./.venv/bin/recommender-stage1 load-source
```

Build derived tables:

```bash
./.venv/bin/recommender-stage1 build-derived
```

Sync external stores when services are running:

```bash
./.venv/bin/recommender-stage1 sync-search
./.venv/bin/recommender-stage1 sync-embeddings
```

## Notes

- The sample data does not include review or stock feeds yet, so `product_stats.review_score`, `product_stats.review_count`, and `product_stats.in_stock` use sensible prototype defaults.
- The code is PostgreSQL-ready, but defaults to SQLite so the sample pipeline can run immediately in a fresh local environment.
