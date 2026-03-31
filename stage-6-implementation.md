# Stage 6: Shared Infrastructure Implementation

## Overview

Stage 6 provides the foundational infrastructure components that support all recommendation features across the platform. This includes:

1. **Event Logging** - Track user interactions (impressions, clicks, purchases)
2. **Feature Flags** - A/B testing and gradual rollouts
3. **Trending Products** - Fast fallback recommendations for anonymous users
4. **Celery Tasks** - Nightly maintenance jobs
5. **CLI Commands** - Infrastructure management commands

---

## 1. Database Models

### Location: `src/onprem_recommenders/models.py`

### Event Model

The `Event` model stores user interaction events for analytics and model retraining.

```python
class Event(Base):
    """Logs user interaction events for analytics and model retraining."""
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    feature: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    product_ids: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array
    query_text: Mapped[str | None] = mapped_column(String(512), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
```

**Fields Explained:**
- `user_id`: User identifier (NULL for anonymous users)
- `feature`: Which feature generated this event (`homepage`, `search`, `product_page`, `autocomplete`)
- `event_type`: Type of interaction (`impression`, `click`, `add_to_cart`, `purchase`)
- `product_ids`: JSON array of product IDs shown/clicked
- `query_text`: Search query (for search/autocomplete events)
- `metadata_json`: Additional context (e.g., variant, position)
- `timestamp`: When the event occurred

**Example Event:**
```json
{
  "id": 1,
  "user_id": "USR_13914CAFA179",
  "feature": "homepage",
  "event_type": "impression",
  "product_ids": "[\"PROD_1\", \"PROD_2\", \"PROD_3\"]",
  "query_text": null,
  "metadata_json": "{\"variant\": \"treatment_a\", \"row\": 1}",
  "timestamp": "2026-03-31T02:48:24.725392"
}
```

### FeatureFlag Model

The `FeatureFlag` model manages A/B testing configurations.

```python
class FeatureFlag(Base):
    """Feature flags for A/B testing and gradual rollouts."""
    __tablename__ = "feature_flags"

    feature_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    variant: Mapped[str] = mapped_column(String(32), nullable=False, default="control")
    user_segment: Mapped[str | None] = mapped_column(String(64), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
```

**Fields Explained:**
- `feature_name`: Unique identifier (e.g., `homepage`, `search_ranking`)
- `variant`: A/B test variant (`control`, `treatment_a`, `treatment_b`)
- `user_segment`: Optional segment targeting (`new_users`, `power_users`)
- `enabled`: Whether the flag is active
- `description`: Human-readable description

**Example Feature Flag:**
```json
{
  "feature_name": "homepage",
  "variant": "treatment_a",
  "user_segment": null,
  "enabled": true,
  "description": "New homepage recommendation algorithm"
}
```

### TrendingProduct Model

The `TrendingProduct` model stores precomputed trending products for fast fallback recommendations.

```python
class TrendingProduct(Base):
    """Precomputed trending products for fallback recommendations."""
    __tablename__ = "trending_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    category_path: Mapped[str | None] = mapped_column(String(512), nullable=True, index=True)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    period: Mapped[str] = mapped_column(String(32), nullable=False)
    computed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
```

**Fields Explained:**
- `product_id`: The trending product
- `category_path`: Category for grouping
- `rank`: Position within category (1 = most popular)
- `score`: Popularity score (higher = more popular)
- `period`: Time period (`hourly`, `daily`)
- `computed_at`: When this was calculated

---

## 2. Event Logging API

### Location: `src/onprem_recommenders/infrastructure.py`

### Endpoint: POST /events

Log user interaction events from frontend applications.

**Request Model:**
```python
class EventRequest(BaseModel):
    user_id: str | None = None           # User ID (anonymous if null)
    feature: str                         # homepage, search, product_page, autocomplete
    event_type: str                      # impression, click, add_to_cart, purchase
    product_ids: list[str] | None = None # Products shown/clicked
    query_text: str | None = None        # Search query (if applicable)
    metadata: dict[str, Any] | None = None  # Additional context
```

**Example Request:**
```bash
curl -X POST "http://127.0.0.1:8000/events" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "USR_123",
    "feature": "homepage",
    "event_type": "impression",
    "product_ids": ["PROD_1", "PROD_2", "PROD_3"],
    "metadata": {"variant": "treatment_a", "row": 1}
  }'
```

**Response:**
```json
{
  "success": true,
  "event_id": 1,
  "message": "Event logged successfully"
}
```

**Event Types:**
| Event Type | Description | Example Use Case |
|------------|-------------|------------------|
| `impression` | Products shown to user | Log what products were displayed |
| `click` | User clicked a product | Track engagement with recommendations |
| `add_to_cart` | User added product to cart | Track conversion funnel |
| `purchase` | User completed purchase | Track final conversion |

**Features:**
| Feature | Description |
|---------|-------------|
| `homepage` | Homepage recommendations |
| `search` | Search results ranking |
| `product_page` | Product page recommenders |
| `autocomplete` | Autocomplete suggestions |

### Internal Helper Function

```python
def log_impression_internal(
    session: Session,
    user_id: str | None,
    feature: str,
    product_ids: list[str],
    query_text: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Internal function to log an impression event.
    
    Called by recommendation endpoints to log what products were shown.
    """
```

This function can be called directly from other API endpoints to log impressions without making HTTP requests.

---

## 3. Feature Flags API

### Location: `src/onprem_recommenders/infrastructure.py`

### Endpoint: GET /feature-flags

List all feature flags and their current state.

**Response:**
```json
{
  "flags": [
    {
      "feature_name": "homepage",
      "variant": "treatment_a",
      "user_segment": null,
      "enabled": true,
      "description": null
    }
  ]
}
```

### Endpoint: GET /feature-flags/{feature_name}

Get a specific feature flag.

**Example:**
```bash
curl "http://127.0.0.1:8000/feature-flags/homepage"
```

### Endpoint: PUT /feature-flags/{feature_name}

Create or update a feature flag.

**Request Model:**
```python
class UpdateFeatureFlagRequest(BaseModel):
    variant: str | None = None           # A/B test variant
    user_segment: str | None = None      # Target segment
    enabled: bool | None = None          # Enable/disable
    description: str | None = None       # Human-readable description
```

**Example:**
```bash
curl -X PUT "http://127.0.0.1:8000/feature-flags/homepage" \
  -H "Content-Type: application/json" \
  -d '{"enabled": true, "variant": "treatment_a"}'
```

### Caching Implementation

Feature flags are cached in memory for 5 minutes to avoid repeated database queries:

```python
# In-memory cache with TTL
_feature_flags_cache: dict[str, tuple[FeatureFlag, datetime]] = {}
_cache_ttl_seconds = 300  # 5 minutes

def get_feature_flag(session: Session, feature_name: str, user_segment: str | None = None) -> FeatureFlag | None:
    # Check cache first
    cache_key = f"{feature_name}:{user_segment or 'default'}"
    if cache_key in _feature_flags_cache:
        flag, cached_at = _feature_flags_cache[cache_key]
        if datetime.utcnow() - cached_at < timedelta(seconds=_cache_ttl_seconds):
            return flag
    # ... query database if cache miss
```

### Helper Functions for Other Modules

```python
def is_feature_enabled(feature_name: str, user_segment: str | None = None, engine: Engine | None = None) -> bool:
    """Check if a feature is enabled. Convenience function for use within other modules."""
    
def get_feature_variant(feature_name: str, user_id: str | None = None, engine: Engine | None = None) -> str:
    """Get the variant for a feature (for A/B testing). Returns 'control' if not found."""
```

**Usage Example:**
```python
# In recommendations.py
from onprem_recommenders.infrastructure import is_feature_enabled, get_feature_variant

if is_feature_enabled("homepage"):
    variant = get_feature_variant("homepage")
    if variant == "treatment_a":
        # Use new algorithm
        return personalized_recommendations_v2(user_id)
    else:
        # Use control algorithm
        return personalized_recommendations_v1(user_id)
```

---

## 4. Trending Products API

### Location: `src/onprem_recommenders/infrastructure.py`

### Endpoint: GET /trending

Get precomputed trending products for fallback recommendations.

**Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `period` | string | "daily" | Trending period (hourly, daily) |
| `categories` | int | 3 | Number of categories (1-10) |
| `products_per_category` | int | 10 | Products per category (1-50) |

**Example:**
```bash
curl "http://127.0.0.1:8000/trending?categories=2&products_per_category=5"
```

**Response:**
```json
{
  "period": "daily",
  "categories": [
    {
      "category": "Electronics > Computers & Accessories > Tablet Accessories",
      "products": [
        {
          "product_id": "B08KXZXCL6",
          "category_path": "Electronics > Computers & Accessories > Tablet Accessories",
          "rank": 1,
          "score": 3.93
        }
      ]
    }
  ],
  "computed_at": "2026-03-31T02:48:24.739842"
}
```

### Trending Computation Function

```python
def compute_trending_products(
    engine: Engine,
    period: str = "daily",
    top_n_per_category: int = 10,
    top_categories: int = 5,
) -> int:
    """Compute and store trending products.
    
    This should be called periodically (hourly/daily) via Celery.
    
    Returns the number of trending products stored.
    """
```

**Algorithm:**
1. Query Elasticsearch for top categories by product count
2. Group categories by root category (e.g., "Electronics")
3. For each top category, fetch top products by popularity score
4. Store in `trending_products` table

### Fast Fallback from Database

```python
def get_trending_by_category_from_db(
    session: Session,
    period: str = "daily",
    categories: int = 3,
    products_per_category: int = 10,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Get precomputed trending products grouped by category.
    
    Fast fallback for anonymous/cold-start users.
    """
```

---

## 5. Health Check Extension

### Endpoint: GET /health/detailed

Extended health check for all dependencies.

**Response:**
```json
{
  "status": "ok",
  "stage": "stage6",
  "database": "ok",
  "elasticsearch": "ok",
  "chromadb": "ok"
}
```

---

## 6. Celery Tasks

### Location: `src/onprem_recommenders/tasks.py`

### Task Configuration

```python
app.conf.beat_schedule = {
    "nightly-derived-tables": {
        "task": "onprem_recommenders.tasks.recompute_derived_tables",
        "schedule": 60 * 60 * 24,  # Every 24 hours
    },
    "nightly-trending-products": {
        "task": "onprem_recommenders.tasks.compute_trending",
        "schedule": 60 * 60,  # Every hour
    },
    "nightly-query-suggestions": {
        "task": "onprem_recommenders.tasks.update_query_suggestions",
        "schedule": 60 * 60 * 24,  # Every 24 hours
    },
}
```

### Available Tasks

| Task | Schedule | Description |
|------|----------|-------------|
| `recompute_derived_tables` | Daily | Update user_category_affinity, product_stats, co_purchase_pairs, co_view_pairs |
| `compute_trending` | Hourly | Recompute trending products from Elasticsearch |
| `update_query_suggestions` | Daily | Update autocomplete query frequencies |
| `sync_embeddings` | On-demand | Sync product embeddings to ChromaDB |
| `run_all_nightly` | Manual | Run all nightly tasks in sequence |

### Running Celery Worker

```bash
# Start Celery worker
celery -A onprem_recommenders.tasks worker --loglevel=info

# Start Celery beat (scheduler)
celery -A onprem_recommenders.tasks beat --loglevel=info
```

---

## 7. CLI Commands

### Location: `src/onprem_recommenders/cli.py`

All infrastructure commands are under the `infra` subcommand:

```bash
recommender-stage1 infra --help
```

### Available Commands

#### compute-trending

Compute and store trending products.

```bash
recommender-stage1 infra compute-trending --period daily --categories 5 --top-n 10
```

**Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `--period` | daily | Trending period (hourly, daily) |
| `--categories` | 5 | Number of top categories |
| `--top-n` | 10 | Products per category |

#### list-events

List recent events from the event log.

```bash
recommender-stage1 infra list-events --feature homepage --event-type impression --limit 20
```

**Output:**
```
Found 2 events:
  [2026-03-31 02:48:24] homepage/impression user=USR_123 products=["PROD_1", "PROD_2"]
  [2026-03-31 02:48:01] homepage/impression user=USR_123 products=["PROD_1", "PROD_2"]
```

#### list-flags

List all feature flags and their status.

```bash
recommender-stage1 infra list-flags
```

**Output:**
```
Found 1 feature flags:
  homepage: treatment_a (enabled) segment=all
```

#### set-flag

Create or update a feature flag.

```bash
recommender-stage1 infra set-flag homepage --enabled True --variant treatment_a --description "New homepage algorithm"
```

#### run-nightly

Run all nightly maintenance tasks manually.

```bash
recommender-stage1 infra run-nightly
```

**Output:**
```
Running nightly maintenance tasks...
  Building derived tables...
    user_category_affinity: 1234 rows
    product_stats: 567 rows
  Computing trending products...
    trending_products: 50 products
  Building query suggestions...
    query_suggestions: 890 suggestions
Nightly maintenance completed!
```

---

## 8. Configuration

### Location: `src/onprem_recommenders/config.py`

### New Settings for Stage 6

```python
class Settings(BaseSettings):
    # ... existing settings ...
    
    # Celery/Redis Configuration
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str | None = None  # Defaults to redis_url
    celery_result_backend: str | None = None  # Defaults to redis_url
    
    @property
    def celery_broker_url_resolved(self) -> str:
        return self.celery_broker_url or self.redis_url
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `CELERY_BROKER_URL` | (REDIS_URL) | Celery broker URL |
| `CELERY_RESULT_BACKEND` | (REDIS_URL) | Celery result backend |

---

## 9. Usage Examples

### Complete Flow Example

```python
# 1. Log an impression from homepage recommendations
import requests

# Log impression
requests.post("http://127.0.0.1:8000/events", json={
    "user_id": "USR_123",
    "feature": "homepage",
    "event_type": "impression",
    "product_ids": ["PROD_1", "PROD_2", "PROD_3"],
    "metadata": {"variant": "treatment_a"}
})

# 2. Check feature flag for personalization algorithm
response = requests.get("http://127.0.0.1:8000/feature-flags/homepage")
flag = response.json()
# Returns: {"feature_name": "homepage", "variant": "treatment_a", "enabled": true}

# 3. Get trending products for anonymous user fallback
response = requests.get("http://127.0.0.1:8000/trending?categories=3&products_per_category=10")
trending = response.json()
# Returns: {"period": "daily", "categories": [...], "computed_at": "..."}
```

### Integration in Recommendations Module

```python
from onprem_recommenders.infrastructure import (
    is_feature_enabled,
    get_feature_variant,
    log_impression_internal,
    get_trending_by_category_from_db,
)

def get_homepage_recommendations(user_id: str | None, session: Session) -> dict:
    # Check feature flag
    if is_feature_enabled("homepage"):
        variant = get_feature_variant("homepage")
        if variant == "treatment_a":
            recommendations = personalized_v2(user_id, session)
        else:
            recommendations = personalized_v1(user_id, session)
    else:
        recommendations = personalized_v1(user_id, session)
    
    # Log impression
    product_ids = [p["product_id"] for row in recommendations["rows"] for p in row["products"]]
    log_impression_internal(
        session=session,
        user_id=user_id,
        feature="homepage",
        product_ids=product_ids,
        metadata={"variant": get_feature_variant("homepage")}
    )
    
    return recommendations
```

---

## 10. Database Schema

### Migration SQL

```sql
-- Events table
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id VARCHAR(64),
    feature VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    product_ids TEXT,
    query_text VARCHAR(512),
    metadata_json TEXT,
    timestamp DATETIME NOT NULL
);
CREATE INDEX idx_events_user_id ON events(user_id);
CREATE INDEX idx_events_feature ON events(feature);
CREATE INDEX idx_events_event_type ON events(event_type);
CREATE INDEX idx_events_timestamp ON events(timestamp);

-- Feature flags table
CREATE TABLE feature_flags (
    feature_name VARCHAR(64) PRIMARY KEY,
    variant VARCHAR(32) NOT NULL DEFAULT 'control',
    user_segment VARCHAR(64),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    description VARCHAR(256),
    created_at DATETIME,
    updated_at DATETIME
);

-- Trending products table
CREATE TABLE trending_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id VARCHAR(64) NOT NULL,
    category_path VARCHAR(512),
    rank INTEGER NOT NULL,
    score FLOAT NOT NULL,
    period VARCHAR(32) NOT NULL,
    computed_at DATETIME
);
CREATE INDEX idx_trending_product_id ON trending_products(product_id);
CREATE INDEX idx_trending_category ON trending_products(category_path);
```

---

## Summary

Stage 6 provides the essential infrastructure layer for the recommendation system:

1. **Event Logging** - Captures all user interactions for analytics and ML model retraining
2. **Feature Flags** - Enables A/B testing and gradual rollouts without code changes
3. **Trending Products** - Precomputed fallback recommendations for anonymous users
4. **Celery Tasks** - Automated nightly maintenance for derived tables and trending
5. **CLI Commands** - Management tools for operations team

All components integrate seamlessly with existing Stages 1-5:
- Events are logged from all recommendation endpoints
- Feature flags control which algorithm variants are used
- Trending products serve as fast fallback for cold-start users
- Nightly tasks keep derived tables and trending products fresh