"""Shared infrastructure for Stage 6: logging, feature flags, and trending.

This module provides:
- Event logging for impressions and clicks
- Feature flag management for A/B testing
- Trending products endpoint for fallbacks
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from elasticsearch import Elasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import delete, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from onprem_recommenders.config import get_settings
from onprem_recommenders.db import create_db_engine
from onprem_recommenders.models import Event, FeatureFlag, TrendingProduct

logger = logging.getLogger(__name__)

router = APIRouter(tags=["infrastructure"])


# =============================================================================
# Event Logging
# =============================================================================


class EventRequest(BaseModel):
    """Request model for logging user events."""

    user_id: str | None = Field(default=None, description="User ID (anonymous if null)")
    feature: str = Field(..., description="Feature name (homepage, search, product_page, autocomplete)")
    event_type: str = Field(..., description="Event type (impression, click, add_to_cart, purchase)")
    product_ids: list[str] | None = Field(default=None, description="List of product IDs shown/clicked")
    query_text: str | None = Field(default=None, description="Search query (if applicable)")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional event metadata")


class EventResponse(BaseModel):
    """Response model for event logging."""

    success: bool
    event_id: int | None = None
    message: str


def get_engine() -> Engine:
    """Get database engine from settings."""
    settings = get_settings()
    return create_db_engine(settings.database_url)


@router.post("/events", response_model=EventResponse)
def log_event(event: EventRequest) -> EventResponse:
    """Log a user interaction event.

    Records impressions (products shown) and user actions (clicks, etc.)
    for analytics and model retraining.

    Event types:
    - impression: Products shown to user (logged by frontend)
    - click: User clicked on a product
    - add_to_cart: User added product to cart
    - purchase: User completed purchase

    Features:
    - homepage: Homepage recommendations
    - search: Search results
    - product_page: Product page recommenders (bought-together, also-viewed)
    - autocomplete: Autocomplete suggestions
    """
    engine = get_engine()

    # Validate event_type
    valid_event_types = {"impression", "click", "add_to_cart", "purchase"}
    if event.event_type not in valid_event_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid event_type. Must be one of: {valid_event_types}",
        )

    # Validate feature
    valid_features = {"homepage", "search", "product_page", "autocomplete"}
    if event.feature not in valid_features:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid feature. Must be one of: {valid_features}",
        )

    with Session(engine) as session:
        db_event = Event(
            user_id=event.user_id,
            feature=event.feature,
            event_type=event.event_type,
            product_ids=json.dumps(event.product_ids) if event.product_ids else None,
            query_text=event.query_text,
            metadata_json=json.dumps(event.metadata) if event.metadata else None,
            timestamp=datetime.utcnow(),
        )
        session.add(db_event)
        session.commit()
        event_id = db_event.id

    logger.info(f"Logged event: feature={event.feature}, type={event.event_type}, user={event.user_id}")

    return EventResponse(
        success=True,
        event_id=event_id,
        message="Event logged successfully",
    )


class ImpressionRequest(BaseModel):
    """Request model for logging impressions from API responses."""

    user_id: str | None = None
    feature: str
    product_ids: list[str]
    query_text: str | None = None
    metadata: dict[str, Any] | None = None


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
    event = Event(
        user_id=user_id,
        feature=feature,
        event_type="impression",
        product_ids=json.dumps(product_ids),
        query_text=query_text,
        metadata_json=json.dumps(metadata) if metadata else None,
        timestamp=datetime.utcnow(),
    )
    session.add(event)


# =============================================================================
# Feature Flags
# =============================================================================


class FeatureFlagResponse(BaseModel):
    """Response model for feature flag."""

    feature_name: str
    variant: str
    user_segment: str | None
    enabled: bool
    description: str | None


class FeatureFlagsListResponse(BaseModel):
    """Response model for listing all feature flags."""

    flags: list[FeatureFlagResponse]


# In-memory cache for feature flags with TTL
_feature_flags_cache: dict[str, tuple[FeatureFlag, datetime]] = {}
_cache_ttl_seconds = 300  # 5 minutes


def get_feature_flag(
    session: Session,
    feature_name: str,
    user_segment: str | None = None,
) -> FeatureFlag | None:
    """Get a feature flag with caching.

    Caches flags in memory for 5 minutes to avoid repeated DB queries.
    """
    # Check cache
    cache_key = f"{feature_name}:{user_segment or 'default'}"
    if cache_key in _feature_flags_cache:
        flag, cached_at = _feature_flags_cache[cache_key]
        if datetime.utcnow() - cached_at < timedelta(seconds=_cache_ttl_seconds):
            return flag

    # Query database
    result = session.execute(
        select(FeatureFlag).where(FeatureFlag.feature_name == feature_name)
    )
    flag = result.scalars().first()

    if flag and flag.enabled:
        # Update cache
        _feature_flags_cache[cache_key] = (flag, datetime.utcnow())

    return flag if flag and flag.enabled else None


def is_feature_enabled(
    feature_name: str,
    user_segment: str | None = None,
    engine: Engine | None = None,
) -> bool:
    """Check if a feature is enabled.

    Convenience function for use within other modules.
    """
    if engine is None:
        engine = get_engine()

    with Session(engine) as session:
        flag = get_feature_flag(session, feature_name, user_segment)
        return flag is not None and flag.enabled


def get_feature_variant(
    feature_name: str,
    user_id: str | None = None,
    engine: Engine | None = None,
) -> str:
    """Get the variant for a feature (for A/B testing).

    Returns the variant name (e.g., "control", "treatment_a").
    Defaults to "control" if flag not found.
    """
    if engine is None:
        engine = get_engine()

    with Session(engine) as session:
        flag = get_feature_flag(session, feature_name)
        if flag and flag.enabled:
            return flag.variant
    return "control"


@router.get("/feature-flags", response_model=FeatureFlagsListResponse)
def list_feature_flags() -> FeatureFlagsListResponse:
    """List all feature flags and their current state."""
    engine = get_engine()

    with Session(engine) as session:
        result = session.execute(select(FeatureFlag))
        flags = result.scalars().all()

    return FeatureFlagsListResponse(
        flags=[
            FeatureFlagResponse(
                feature_name=f.feature_name,
                variant=f.variant,
                user_segment=f.user_segment,
                enabled=f.enabled,
                description=f.description,
            )
            for f in flags
        ]
    )


@router.get("/feature-flags/{feature_name}", response_model=FeatureFlagResponse)
def get_feature_flag_endpoint(feature_name: str) -> FeatureFlagResponse:
    """Get a specific feature flag."""
    engine = get_engine()

    with Session(engine) as session:
        flag = session.execute(
            select(FeatureFlag).where(FeatureFlag.feature_name == feature_name)
        ).scalars().first()

    if not flag:
        raise HTTPException(status_code=404, detail=f"Feature flag '{feature_name}' not found")

    return FeatureFlagResponse(
        feature_name=flag.feature_name,
        variant=flag.variant,
        user_segment=flag.user_segment,
        enabled=flag.enabled,
        description=flag.description,
    )


class UpdateFeatureFlagRequest(BaseModel):
    """Request model for updating a feature flag."""

    variant: str | None = None
    user_segment: str | None = None
    enabled: bool | None = None
    description: str | None = None


@router.put("/feature-flags/{feature_name}", response_model=FeatureFlagResponse)
def update_feature_flag(
    feature_name: str,
    request: UpdateFeatureFlagRequest,
) -> FeatureFlagResponse:
    """Update a feature flag.

    Creates the flag if it doesn't exist.
    """
    engine = get_engine()

    with Session(engine) as session:
        flag = session.execute(
            select(FeatureFlag).where(FeatureFlag.feature_name == feature_name)
        ).scalars().first()

        if flag:
            # Update existing
            if request.variant is not None:
                flag.variant = request.variant
            if request.user_segment is not None:
                flag.user_segment = request.user_segment
            if request.enabled is not None:
                flag.enabled = request.enabled
            if request.description is not None:
                flag.description = request.description
            flag.updated_at = datetime.utcnow()
        else:
            # Create new
            flag = FeatureFlag(
                feature_name=feature_name,
                variant=request.variant or "control",
                user_segment=request.user_segment,
                enabled=request.enabled if request.enabled is not None else True,
                description=request.description,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            session.add(flag)

        session.commit()

        # Extract values before session closes
        response = FeatureFlagResponse(
            feature_name=flag.feature_name,
            variant=flag.variant,
            user_segment=flag.user_segment,
            enabled=flag.enabled,
            description=flag.description,
        )

        # Clear cache for this flag
        keys_to_remove = [k for k in _feature_flags_cache if k.startswith(feature_name)]
        for key in keys_to_remove:
            del _feature_flags_cache[key]

    return response


# =============================================================================
# Trending Products
# =============================================================================


class TrendingProductItem(BaseModel):
    """Single trending product."""

    product_id: str
    title: str
    brand: str
    price: float
    category_path: str
    popularity_score: float
    rank: int


class TrendingResponse(BaseModel):
    """Response model for trending products."""

    period: str
    categories: list[dict[str, Any]]
    computed_at: datetime | None


def get_elasticsearch_client() -> Elasticsearch:
    """Get Elasticsearch client."""
    settings = get_settings()
    return Elasticsearch(
        settings.elasticsearch_url,
        verify_certs=False,
        ssl_show_warn=False,
    )


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
    client = get_elasticsearch_client()
    settings = get_settings()

    # Get top categories from Elasticsearch
    aggs_query = {
        "size": 0,
        "aggs": {
            "categories": {
                "terms": {
                    "field": "category_path.keyword",
                    "size": 100,
                }
            }
        }
    }

    response = client.search(index=settings.elasticsearch_index, body=aggs_query)
    buckets = response.get("aggregations", {}).get("categories", {}).get("buckets", [])

    # Group by root category
    root_categories: dict[str, int] = {}
    for bucket in buckets:
        cat = bucket["key"]
        root = cat.split(" > ")[0] if " > " in cat else cat
        if root not in root_categories:
            root_categories[root] = bucket["doc_count"]

    # Sort and take top N
    sorted_categories = sorted(
        root_categories.items(), key=lambda x: x[1], reverse=True
    )[:top_categories]

    # Clear existing trending products for this period
    with Session(engine) as session:
        session.execute(
            delete(TrendingProduct).where(TrendingProduct.period == period)
        )
        session.commit()

    # Fetch top products per category
    stored_count = 0
    computed_at = datetime.utcnow()

    for category, _ in sorted_categories:
        query = {
            "query": {
                "match": {"category_path": category}
            },
            "sort": [
                {"popularity_score": {"order": "desc"}}
            ],
            "size": top_n_per_category,
            "_source": ["product_id", "category_path", "popularity_score"],
        }

        response = client.search(index=settings.elasticsearch_index, body=query)
        hits = response.get("hits", {}).get("hits", [])

        with Session(engine) as session:
            for rank, hit in enumerate(hits, start=1):
                source = hit["_source"]
                trending = TrendingProduct(
                    product_id=source["product_id"],
                    category_path=source.get("category_path"),
                    rank=rank,
                    score=source.get("popularity_score", 0.0),
                    period=period,
                    computed_at=computed_at,
                )
                session.add(trending)
                stored_count += 1

            session.commit()

    logger.info(f"Computed {stored_count} trending products for period '{period}'")
    return stored_count


def get_trending_from_db(
    session: Session,
    period: str = "daily",
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Get precomputed trending products from database.

    Fast fallback for anonymous/cold-start users.
    """
    result = session.execute(
        select(TrendingProduct)
        .where(TrendingProduct.period == period)
        .order_by(TrendingProduct.rank)
        .limit(limit)
    )
    return [
        {
            "product_id": row.product_id,
            "category_path": row.category_path,
            "rank": row.rank,
            "score": row.score,
        }
        for row in result.scalars().all()
    ]


def get_trending_by_category_from_db(
    session: Session,
    period: str = "daily",
    categories: int = 3,
    products_per_category: int = 10,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Get precomputed trending products grouped by category.

    Returns list of (category, products) tuples.
    """
    # Get distinct categories
    result = session.execute(
        select(TrendingProduct.category_path)
        .where(TrendingProduct.period == period)
        .distinct()
        .limit(categories)
    )
    category_list = [row[0] for row in result.fetchall() if row[0]]

    output = []
    for category in category_list:
        cat_result = session.execute(
            select(TrendingProduct)
            .where(TrendingProduct.period == period)
            .where(TrendingProduct.category_path == category)
            .order_by(TrendingProduct.rank)
            .limit(products_per_category)
        )
        products = [
            {
                "product_id": row.product_id,
                "category_path": row.category_path,
                "rank": row.rank,
                "score": row.score,
            }
            for row in cat_result.scalars().all()
        ]
        if products:
            output.append((category, products))

    return output


@router.get("/trending", response_model=TrendingResponse)
def get_trending_products(
    period: str = Query("daily", description="Trending period (hourly, daily)"),
    categories: int = Query(3, ge=1, le=10, description="Number of categories"),
    products_per_category: int = Query(10, ge=1, le=50, description="Products per category"),
) -> TrendingResponse:
    """Get precomputed trending products.

    Fast endpoint for fallback recommendations to anonymous
    and cold-start users. Data is precomputed by Celery jobs.
    """
    engine = get_engine()
    client = get_elasticsearch_client()
    settings = get_settings()

    with Session(engine) as session:
        trending_data = get_trending_by_category_from_db(
            session,
            period=period,
            categories=categories,
            products_per_category=products_per_category,
        )

    # If no precomputed data, fall back to live computation
    if not trending_data:
        # Fetch from Elasticsearch directly
        aggs_query = {
            "size": 0,
            "aggs": {
                "categories": {
                    "terms": {
                        "field": "category_path.keyword",
                        "size": 100,
                    }
                }
            }
        }

        response = client.search(index=settings.elasticsearch_index, body=aggs_query)
        buckets = response.get("aggregations", {}).get("categories", {}).get("buckets", [])

        # Group by root category
        root_categories: dict[str, int] = {}
        for bucket in buckets:
            cat = bucket["key"]
            root = cat.split(" > ")[0] if " > " in cat else cat
            if root not in root_categories:
                root_categories[root] = bucket["doc_count"]

        sorted_categories = sorted(
            root_categories.items(), key=lambda x: x[1], reverse=True
        )[:categories]

        trending_data = []
        for category, _ in sorted_categories:
            query = {
                "query": {"match": {"category_path": category}},
                "sort": [{"popularity_score": {"order": "desc"}}],
                "size": products_per_category,
                "_source": ["product_id", "title", "brand", "price", "category_path", "popularity_score"],
            }

            response = client.search(index=settings.elasticsearch_index, body=query)
            hits = response.get("hits", {}).get("hits", [])

            products = []
            for hit in hits:
                source = hit["_source"]
                products.append({
                    "product_id": source["product_id"],
                    "title": source["title"],
                    "brand": source["brand"],
                    "price": source["price"],
                    "category_path": source["category_path"],
                    "popularity_score": source.get("popularity_score", 0.0),
                })

            if products:
                trending_data.append((category, products))

    return TrendingResponse(
        period=period,
        categories=[
            {
                "category": cat,
                "products": products,
            }
            for cat, products in trending_data
        ],
        computed_at=datetime.utcnow(),
    )


# =============================================================================
# Health Check Extension
# =============================================================================


class HealthResponse(BaseModel):
    """Extended health check response."""

    status: str
    stage: str
    database: str
    elasticsearch: str
    chromadb: str


def check_database_health(engine: Engine) -> str:
    """Check database connectivity."""
    try:
        with Session(engine) as session:
            session.execute(select(1))
        return "ok"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return f"error: {str(e)[:50]}"


def check_elasticsearch_health(client: Elasticsearch) -> str:
    """Check Elasticsearch connectivity."""
    try:
        client.ping()
        return "ok"
    except Exception as e:
        logger.error(f"Elasticsearch health check failed: {e}")
        return f"error: {str(e)[:50]}"


def check_chromadb_health() -> str:
    """Check ChromaDB connectivity."""
    try:
        import chromadb
        settings = get_settings()
        client = chromadb.PersistentClient(path=str(settings.chroma_persist_directory))
        client.get_collection(settings.chroma_collection)
        return "ok"
    except Exception as e:
        logger.error(f"ChromaDB health check failed: {e}")
        return f"error: {str(e)[:50]}"


@router.get("/health/detailed", response_model=HealthResponse)
def detailed_health_check() -> HealthResponse:
    """Detailed health check including all dependencies."""
    engine = get_engine()
    es_client = get_elasticsearch_client()

    return HealthResponse(
        status="ok",
        stage="stage6",
        database=check_database_health(engine),
        elasticsearch=check_elasticsearch_health(es_client),
        chromadb=check_chromadb_health(),
    )