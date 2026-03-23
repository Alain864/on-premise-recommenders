# Search & Recommendation Prototype Plan

## Goal

Build a local working prototype with:

- Personalized homepage recommendations
- Personalized query autocomplete
- Search results ranking
- Product page recommendations
- Minimal but polished Rust frontend

The prototype will start from:

- `200,000` products
- `2` months of user interaction and transaction history
- Input files shaped like `sample_data.xlsx`:
  - `Users`: `user_id`, `signup_date`, `country`
  - `Products`: `product_id`, `title`, `brand`, `price`, `category_path`, `description`
  - `Transactions`: `order_id`, `user_id`, `product_id`, `timestamp`
  - `Interactions`: `event_type`, `user_id`, `product_id`, `query_text`, `timestamp`

## Recommended Stack

- Backend API: `FastAPI`, `Pydantic`, `Uvicorn`
- Data processing: `Polars`, `PyArrow`
- ML / ranking: `scikit-learn`, `implicit`, `sentence-transformers`
- Orchestration layer: `LangChain` only where it adds value for retrieval pipelines, not for core ranking logic
- Vector store: `ChromaDB`
- Lexical search and autocomplete: `Meilisearch` for the local prototype
- Local analytics / feature tables: `DuckDB`
- Model and job packaging: plain Python modules plus CLI scripts
- Frontend: `Leptos` for a small Rust web UI
- Experiment flags and config: `.env` + YAML or TOML config files
- Observability: structured JSON logs

## Technical Stages

### 1. Define the local architecture

- Keep the system split into four parts:
  - offline data pipeline
  - indexes and feature stores
  - online inference API
  - Rust frontend
- Use `DuckDB` as the local source for cleaned tables and aggregated features.
- Use `Meilisearch` for prefix search and lexical retrieval.
- Use `ChromaDB` for semantic product retrieval from embeddings.
- Keep all model outputs materialized so the API mostly reads precomputed results.

### 2. Build the ingestion and normalization pipeline

- Create import scripts for the four source datasets.
- Normalize timestamps, deduplicate rows, and validate required fields.
- Split `Interactions` into separate logical datasets:
  - page views
  - add-to-carts
  - searches
- Derive session ids from user events using a fixed inactivity window.
- Create cleaned tables for users, products, transactions, sessions, and searches.
- Export curated tables to `Parquet` and register them in `DuckDB`.

### 3. Create shared feature tables

- Product features:
  - normalized title and description text
  - category tokens
  - product popularity from views, carts, purchases
  - recency-weighted popularity
- User features:
  - recent views
  - recent purchases
  - top categories
  - brand affinity
  - price preference bands
- Query features:
  - prefix counts
  - global trending queries
  - segment-level trending queries
- Session features:
  - co-view pairs
  - ordered view sequences
- Prepare cold-start fallback tables:
  - trending products
  - bestsellers
  - trending queries by prefix

### 4. Build the retrieval indexes

- Load product catalog into `Meilisearch` for keyword search and fast prefix matching.
- Generate product embeddings from title, brand, category, and description.
- Store embeddings in `ChromaDB`.
- Keep the embedding model configurable:
  - default: local `sentence-transformers` model
  - optional: OpenAI embeddings if quality is better and cost is acceptable
- Precompute nearest-neighbor product candidates for faster online ranking.

### 5. Implement the four recommendation features

#### Homepage feed

- Start with item-item collaborative filtering using purchases and views.
- Add category-affinity and brand-affinity scoring for cold or sparse users.
- Return several labeled rails such as:
  - based on recent views
  - because you bought
  - trending in your categories

#### Personalized autocomplete

- Use `Meilisearch` prefix search over historical queries.
- Re-rank suggestions with:
  - user category affinity
  - user recent searches
  - global and recent query popularity
- Return `5-8` suggestions with a fallback to global trending prefixes.

#### Search ranking

- Retrieve a candidate set from:
  - lexical search in `Meilisearch`
  - semantic search in `ChromaDB`
- Merge candidates and apply a weighted re-ranker using:
  - text relevance
  - semantic similarity
  - popularity
  - add-to-cart and purchase signals
  - in-stock flag when available
  - review metrics when available
- Keep personalization optional behind a flag for the first version.

#### Product page recommenders

- Frequently Bought Together:
  - build co-purchase rules from shared `order_id`
  - rank by support and confidence
- Customers Also Viewed:
  - build co-view relationships from same-session product views
  - optionally train simple item embeddings on view sequences
- Exclude the current product and unavailable products.

### 6. Expose online APIs

- Create FastAPI endpoints for:
  - homepage recommendations
  - autocomplete
  - search
  - product page recommendations
  - event logging
- Keep response contracts simple and frontend-ready.
- Add feature flags so each strategy can be swapped or disabled.
- Make online endpoints read from precomputed artifacts whenever possible.

### 7. Add logging and evaluation hooks

- Log impressions, clicks, search requests, and recommendation source.
- Store logs locally in append-only files or a small local database.
- Add offline evaluation scripts for:
  - precision at k
  - recall at k
  - MRR or NDCG for search ranking
  - coverage and diversity for recommendations
- Keep evaluation reproducible from the curated datasets.

### 8. Build the minimal Rust frontend

- Use `Leptos` to build a small web UI with:
  - homepage with recommendation rails
  - search bar with autocomplete
  - search results page
  - product detail page with the two recommendation modules
- Keep UI minimal:
  - text-first cards
  - no product images
  - clear typography
  - strong spacing and responsive layout
- Frontend should call FastAPI directly over JSON.

### 9. Prepare local run workflow

- Add one command to rebuild data artifacts and indexes.
- Add one command to start backend services.
- Add one command to run the Rust frontend.
- Keep configuration local-first:
  - local embedding model by default
  - optional OpenAI key via environment variable
  - optional GPU usage for embedding generation only

## Important Prototype Decisions

- Use precomputed recommendations and feature tables first; do not make the API compute heavy ranking logic on request.
- Use hybrid retrieval for search from the beginning: lexical plus semantic.
- Keep OpenAI usage optional and isolated to embeddings or later experimentation.
- Do not depend on images or rich merchandising data for the first version.
- If inventory, review score, or review count are not available in the real input, stub those fields behind defaults and keep them pluggable.

## Deliverables To Produce

- Data ingestion and normalization scripts
- Feature generation jobs
- Search and vector indexes
- Recommendation and ranking modules
- FastAPI service
- Leptos frontend
- Local config and run scripts
- Evaluation scripts
- Example seed data and API contracts
