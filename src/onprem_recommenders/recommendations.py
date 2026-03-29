"""Recommendation endpoints for the on-premise recommender system."""

from __future__ import annotations

import logging
from typing import Any

from elasticsearch import Elasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from onprem_recommenders.config import get_settings
from onprem_recommenders.db import create_db_engine
from onprem_recommenders.models import (
    Product,
    ProductStats,
    Transaction,
    UserCategoryAffinity,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/recommendations", tags=["recommendations"])


# Response models
class ProductItem(BaseModel):
    product_id: str
    title: str
    brand: str
    price: float
    category_path: str
    popularity_score: float


class RecommendationRow(BaseModel):
    row_label: str
    products: list[ProductItem]


class HomepageResponse(BaseModel):
    user_id: str
    rows: list[RecommendationRow]
    is_personalized: bool


def get_engine() -> Engine:
    """Get database engine from settings."""
    settings = get_settings()
    return create_db_engine(settings.database_url)


def get_elasticsearch_client() -> Elasticsearch:
    """Get Elasticsearch client."""
    settings = get_settings()
    return Elasticsearch(
        settings.elasticsearch_url,
        verify_certs=False,
        ssl_show_warn=False,
    )


def get_user_top_categories(
    session: Session, user_id: str, limit: int = 3
) -> list[tuple[str, float]]:
    """Get user's top categories by affinity score.

    Returns list of (category_path, affinity_score) tuples.
    """
    result = session.execute(
        select(UserCategoryAffinity)
        .where(UserCategoryAffinity.user_id == user_id)
        .order_by(UserCategoryAffinity.affinity_score.desc())
        .limit(limit)
    )
    return [(row.category_path, row.affinity_score) for row in result.scalars().all()]


def get_user_purchased_product_ids(session: Session, user_id: str) -> set[str]:
    """Get set of product IDs the user has already purchased."""
    result = session.execute(
        select(Transaction.product_id).where(Transaction.user_id == user_id)
    )
    return {row[0] for row in result.fetchall()}


def fetch_products_by_category(
    client: Elasticsearch,
    category_path: str,
    excluded_ids: set[str],
    size: int = 10,
    index_name: str = "products",
) -> list[dict[str, Any]]:
    """Fetch top products for a category from Elasticsearch.

    Filters out already-purchased products and sorts by popularity_score.
    """
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"category_path": category_path}}
                ],
                "must_not": [
                    {"terms": {"product_id": list(excluded_ids)}} if excluded_ids else {}
                ],
            }
        },
        "sort": [
            {"popularity_score": {"order": "desc"}}
        ],
        "size": size,
    }

    # Remove empty must_not clause
    if not excluded_ids:
        query["query"]["bool"].pop("must_not")

    response = client.search(index=index_name, body=query)
    
    products = []
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        products.append({
            "product_id": source["product_id"],
            "title": source["title"],
            "brand": source["brand"],
            "price": source["price"],
            "category_path": source["category_path"],
            "popularity_score": source.get("popularity_score", 0.0),
        })
    
    return products


def fetch_trending_products(
    client: Elasticsearch,
    size: int = 10,
    index_name: str = "products",
) -> list[dict[str, Any]]:
    """Fetch trending/bestseller products globally.

    Fallback for anonymous or cold-start users.
    """
    query = {
        "query": {"match_all": {}},
        "sort": [
            {"popularity_score": {"order": "desc"}}
        ],
        "size": size,
    }

    response = client.search(index=index_name, body=query)
    
    products = []
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        products.append({
            "product_id": source["product_id"],
            "title": source["title"],
            "brand": source["brand"],
            "price": source["price"],
            "category_path": source["category_path"],
            "popularity_score": source.get("popularity_score", 0.0),
        })
    
    return products


def fetch_trending_by_category(
    client: Elasticsearch,
    index_name: str = "products",
    top_n: int = 3,
    products_per_category: int = 10,
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Fetch top products per top-level category.

    Used for cold-start user fallback.
    """
    # Use keyword field for aggregation (efficient for exact matches)
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

    response = client.search(index=index_name, body=aggs_query)

    # Extract top categories
    buckets = response.get("aggregations", {}).get("categories", {}).get("buckets", [])
    # Get root categories (first level)
    root_categories = {}
    for bucket in buckets:
        cat = bucket["key"]
        root = cat.split(" > ")[0] if " > " in cat else cat
        if root not in root_categories:
            root_categories[root] = bucket["doc_count"]

    # Sort by doc_count and take top N
    sorted_categories = sorted(
        root_categories.items(), key=lambda x: x[1], reverse=True
    )[:top_n]

    result = []
    for category, _ in sorted_categories:
        products = fetch_products_by_category(
            client, category, excluded_ids=set(), size=products_per_category, index_name=index_name
        )
        if products:
            result.append((category, products))

    return result


@router.get("/homepage", response_model=HomepageResponse)
def get_homepage_recommendations(
    user_id: str | None = Query(None, description="User ID for personalization"),
    rows: int = Query(3, ge=1, le=10, description="Number of recommendation rows"),
    products_per_row: int = Query(10, ge=1, le=50, description="Products per row"),
) -> HomepageResponse:
    """Get personalized homepage recommendations.

    For known users: returns products from top affinity categories,
    excluding already-purchased items.

    For anonymous/cold-start users: returns trending products by category.
    """
    engine = get_engine()
    client = get_elasticsearch_client()
    settings = get_settings()

    # Anonymous user - return trending products
    if user_id is None:
        trending_rows = fetch_trending_by_category(
            client,
            index_name=settings.elasticsearch_index,
            top_n=rows,
            products_per_category=products_per_row,
        )
        return HomepageResponse(
            user_id="anonymous",
            rows=[
                RecommendationRow(
                    row_label=f"Trending in {category}",
                    products=[ProductItem(**p) for p in products],
                )
                for category, products in trending_rows
            ],
            is_personalized=False,
        )

    # Known user - try personalized recommendations
    with Session(engine) as session:
        user_categories = get_user_top_categories(session, user_id, limit=rows)
        purchased_ids = get_user_purchased_product_ids(session, user_id)

    if not user_categories:
        # Cold-start user - fall back to trending
        trending_rows = fetch_trending_by_category(
            client,
            index_name=settings.elasticsearch_index,
            top_n=rows,
            products_per_category=products_per_row,
        )
        return HomepageResponse(
            user_id=user_id,
            rows=[
                RecommendationRow(
                    row_label=f"Trending in {category}",
                    products=[ProductItem(**p) for p in products],
                )
                for category, products in trending_rows
            ],
            is_personalized=False,
        )

    # Personalized recommendations
    recommendation_rows = []
    for category_path, affinity_score in user_categories:
        products = fetch_products_by_category(
            client,
            category_path,
            excluded_ids=purchased_ids,
            size=products_per_row,
            index_name=settings.elasticsearch_index,
        )

        if products:
            # Create a friendly label from the category
            label_parts = category_path.split(" > ")
            row_label = f"Recommended in {label_parts[-1]}"
            if len(label_parts) > 1:
                row_label = f"Recommended in {label_parts[-1]} ({label_parts[0]})"

            recommendation_rows.append(
                RecommendationRow(
                    row_label=row_label,
                    products=[ProductItem(**p) for p in products],
                )
            )

    # If we got fewer rows than requested, supplement with trending
    if len(recommendation_rows) < rows:
        remaining = rows - len(recommendation_rows)
        trending = fetch_trending_by_category(
            client,
            index_name=settings.elasticsearch_index,
            top_n=remaining,
            products_per_category=products_per_row,
        )
        for category, products in trending:
            recommendation_rows.append(
                RecommendationRow(
                    row_label=f"Trending in {category}",
                    products=[ProductItem(**p) for p in products],
                )
            )

    return HomepageResponse(
        user_id=user_id,
        rows=recommendation_rows[:rows],
        is_personalized=True,
    )