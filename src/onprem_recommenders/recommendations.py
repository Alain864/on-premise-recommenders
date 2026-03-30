"""Recommendation endpoints for the on-premise recommender system."""

from __future__ import annotations

import logging
from typing import Any

import chromadb
from elasticsearch import Elasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query
from openai import OpenAI
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from onprem_recommenders.config import get_settings
from onprem_recommenders.db import create_db_engine
from onprem_recommenders.models import (
    CoPurchasePair,
    CoViewPair,
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


# Product Page Recommenders Response Models
class ProductPageResponse(BaseModel):
    """Response model for product page recommendations."""

    product_id: str
    recommendations: list[ProductItem]
    recommendation_type: str
    fallback: bool


def get_co_purchase_products(
    session: Session, product_id: str, limit: int = 10
) -> list[tuple[str, int]]:
    """Get products frequently bought together with the given product.

    Returns list of (product_id, pair_count) tuples sorted by pair_count descending.
    """
    # Query both left and right sides of the co-purchase pair
    # Left side: current product is left_product_id
    result_left = session.execute(
        select(CoPurchasePair)
        .where(CoPurchasePair.left_product_id == product_id)
        .order_by(CoPurchasePair.pair_count.desc())
        .limit(limit)
    )
    left_pairs = [(row.right_product_id, row.pair_count) for row in result_left.scalars().all()]

    # Right side: current product is right_product_id
    result_right = session.execute(
        select(CoPurchasePair)
        .where(CoPurchasePair.right_product_id == product_id)
        .order_by(CoPurchasePair.pair_count.desc())
        .limit(limit)
    )
    right_pairs = [(row.left_product_id, row.pair_count) for row in result_right.scalars().all()]

    # Merge and deduplicate, keeping highest count
    merged: dict[str, int] = {}
    for pid, count in left_pairs + right_pairs:
        if pid not in merged or merged[pid] < count:
            merged[pid] = count

    # Sort by count descending and return
    return sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]


def get_co_view_products(
    session: Session, product_id: str, limit: int = 10
) -> list[tuple[str, int]]:
    """Get products frequently viewed together with the given product.

    Returns list of (product_id, pair_count) tuples sorted by pair_count descending.
    """
    # Query both left and right sides of the co-view pair
    result_left = session.execute(
        select(CoViewPair)
        .where(CoViewPair.left_product_id == product_id)
        .order_by(CoViewPair.pair_count.desc())
        .limit(limit)
    )
    left_pairs = [(row.right_product_id, row.pair_count) for row in result_left.scalars().all()]

    result_right = session.execute(
        select(CoViewPair)
        .where(CoViewPair.right_product_id == product_id)
        .order_by(CoViewPair.pair_count.desc())
        .limit(limit)
    )
    right_pairs = [(row.left_product_id, row.pair_count) for row in result_right.scalars().all()]

    # Merge and deduplicate, keeping highest count
    merged: dict[str, int] = {}
    for pid, count in left_pairs + right_pairs:
        if pid not in merged or merged[pid] < count:
            merged[pid] = count

    return sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]


def fetch_products_by_ids(
    client: Elasticsearch,
    product_ids: list[str],
    index_name: str = "products",
) -> list[dict[str, Any]]:
    """Fetch products by IDs from Elasticsearch, preserving order."""
    if not product_ids:
        return []

    query = {
        "query": {
            "terms": {
                "product_id": product_ids
            }
        },
        "size": len(product_ids),
    }

    response = client.search(index=index_name, body=query)

    # Create a map for quick lookup
    products_map: dict[str, dict[str, Any]] = {}
    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        products_map[source["product_id"]] = {
            "product_id": source["product_id"],
            "title": source["title"],
            "brand": source["brand"],
            "price": source["price"],
            "category_path": source["category_path"],
            "popularity_score": source.get("popularity_score", 0.0),
        }

    # Return in requested order (filtering out missing products)
    return [products_map[pid] for pid in product_ids if pid in products_map]


def get_product_category(
    client: Elasticsearch,
    product_id: str,
    index_name: str = "products",
) -> str | None:
    """Get the category path for a product."""
    query = {
        "query": {
            "term": {
                "product_id": product_id
            }
        },
        "_source": ["category_path"],
        "size": 1,
    }

    response = client.search(index=index_name, body=query)
    hits = response.get("hits", {}).get("hits", [])
    if hits:
        return hits[0]["_source"].get("category_path")
    return None


@router.get("/product/{product_id}/frequently-bought-together", response_model=ProductPageResponse)
def get_frequently_bought_together(
    product_id: str,
    limit: int = Query(10, ge=1, le=50, description="Number of recommendations"),
) -> ProductPageResponse:
    """Get products frequently bought together with the given product.

    Cross-sell recommendation module for product detail pages.

    Uses co-purchase data to find products that are commonly purchased
    together. Falls back to products from the same category sorted by
    popularity if no co-purchase data exists.
    """
    engine = get_engine()
    client = get_elasticsearch_client()
    settings = get_settings()

    # Get co-purchase products from database
    with Session(engine) as session:
        co_purchase_pairs = get_co_purchase_products(session, product_id, limit=limit)

    # If we have co-purchase data, fetch product details
    if co_purchase_pairs:
        product_ids = [pid for pid, _ in co_purchase_pairs]
        products = fetch_products_by_ids(
            client,
            product_ids,
            index_name=settings.elasticsearch_index,
        )

        if products:
            return ProductPageResponse(
                product_id=product_id,
                recommendations=[ProductItem(**p) for p in products],
                recommendation_type="frequently_bought_together",
                fallback=False,
            )

    # Fallback: Get products from the same category
    category_path = get_product_category(
        client, product_id, index_name=settings.elasticsearch_index
    )

    if category_path:
        fallback_products = fetch_products_by_category(
            client,
            category_path,
            excluded_ids={product_id},
            size=limit,
            index_name=settings.elasticsearch_index,
        )

        if fallback_products:
            return ProductPageResponse(
                product_id=product_id,
                recommendations=[ProductItem(**p) for p in fallback_products],
                recommendation_type="frequently_bought_together",
                fallback=True,
            )

    # Last resort: trending products
    trending = fetch_trending_products(
        client,
        size=limit,
        index_name=settings.elasticsearch_index,
    )
    # Filter out the current product
    trending = [p for p in trending if p["product_id"] != product_id]

    return ProductPageResponse(
        product_id=product_id,
        recommendations=[ProductItem(**p) for p in trending[:limit]],
        recommendation_type="frequently_bought_together",
        fallback=True,
    )


@router.get("/product/{product_id}/customers-also-viewed", response_model=ProductPageResponse)
def get_customers_also_viewed(
    product_id: str,
    limit: int = Query(10, ge=1, le=50, description="Number of recommendations"),
) -> ProductPageResponse:
    """Get products frequently viewed together with the given product.

    Up-sell recommendation module for product detail pages.

    Uses co-view data to find products that are commonly viewed in the
    same session. Falls back to products from related categories sorted by
    popularity if no co-view data exists.
    """
    engine = get_engine()
    client = get_elasticsearch_client()
    settings = get_settings()

    # Get co-view products from database
    with Session(engine) as session:
        co_view_pairs = get_co_view_products(session, product_id, limit=limit)

    # If we have co-view data, fetch product details
    if co_view_pairs:
        product_ids = [pid for pid, _ in co_view_pairs]
        products = fetch_products_by_ids(
            client,
            product_ids,
            index_name=settings.elasticsearch_index,
        )

        if products:
            return ProductPageResponse(
                product_id=product_id,
                recommendations=[ProductItem(**p) for p in products],
                recommendation_type="customers_also_viewed",
                fallback=False,
            )

    # Fallback: Get products from the same category
    category_path = get_product_category(
        client, product_id, index_name=settings.elasticsearch_index
    )

    if category_path:
        fallback_products = fetch_products_by_category(
            client,
            category_path,
            excluded_ids={product_id},
            size=limit,
            index_name=settings.elasticsearch_index,
        )

        if fallback_products:
            return ProductPageResponse(
                product_id=product_id,
                recommendations=[ProductItem(**p) for p in fallback_products],
                recommendation_type="customers_also_viewed",
                fallback=True,
            )

    # Last resort: trending products
    trending = fetch_trending_products(
        client,
        size=limit,
        index_name=settings.elasticsearch_index,
    )
    # Filter out the current product
    trending = [p for p in trending if p["product_id"] != product_id]

    return ProductPageResponse(
        product_id=product_id,
        recommendations=[ProductItem(**p) for p in trending[:limit]],
        recommendation_type="customers_also_viewed",
        fallback=True,
    )


# Search Results Ranking Models
class SearchProductItem(BaseModel):
    """Product item with search ranking details."""

    product_id: str
    title: str
    brand: str
    price: float
    category_path: str
    popularity_score: float
    bm25_score: float | None = None
    final_score: float | None = None


class SearchResponse(BaseModel):
    """Response model for search results."""

    query: str
    user_id: str | None
    results: list[SearchProductItem]
    total_hits: int
    is_personalized: bool
    used_semantic_fallback: bool


def get_product_stats_batch(
    session: Session, product_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Get product stats for a batch of products.

    Returns dict mapping product_id to stats dict.
    """
    result = session.execute(
        select(ProductStats).where(ProductStats.product_id.in_(product_ids))
    )
    return {
        row.product_id: {
            "view_count": row.view_count,
            "conversion_rate": row.conversion_rate,
            "review_score": row.review_score or 3.0,  # Default neutral review
            "in_stock": row.in_stock,
            "popularity_score": row.popularity_score,
        }
        for row in result.scalars().all()
    }


def compute_ranking_score(
    bm25_score: float,
    popularity_score: float,
    conversion_rate: float,
    review_score: float,
    in_stock: bool,
    weights: dict[str, float] | None = None,
) -> float:
    """Compute final ranking score using weighted combination.

    Default weights prioritize:
    - BM25 relevance (40%)
    - Popularity (25%)
    - Conversion rate (15%)
    - Review score (10%)
    - In-stock boost (10%)
    """
    if weights is None:
        weights = {
            "bm25": 0.40,
            "popularity": 0.25,
            "conversion": 0.15,
            "review": 0.10,
            "in_stock": 0.10,
        }

    # Normalize scores to 0-1 range
    # BM25 score can vary widely, normalize relative to max
    # Popularity score is log-scaled already, normalize to 0-1
    popularity_normalized = min(popularity_score / 5.0, 1.0)  # Assume max ~5

    # Conversion rate is already 0-1
    # Review score normalize from 1-5 to 0-1
    review_normalized = (review_score - 1.0) / 4.0 if review_score else 0.5

    # In-stock boost
    in_stock_boost = 1.0 if in_stock else 0.5

    final_score = (
        weights["bm25"] * bm25_score +
        weights["popularity"] * popularity_normalized +
        weights["conversion"] * conversion_rate +
        weights["review"] * review_normalized +
        weights["in_stock"] * in_stock_boost
    )

    return final_score


def get_embedding(text: str) -> list[float]:
    """Get embedding for text using OpenAI."""
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key)

    response = client.embeddings.create(
        input=text,
        model=settings.openai_embedding_model,
    )
    return response.data[0].embedding


def semantic_search_products(
    query_embedding: list[float],
    limit: int = 50,
) -> list[tuple[str, float]]:
    """Search products using vector similarity in ChromaDB.

    Returns list of (product_id, similarity_score) tuples.
    """
    settings = get_settings()
    chroma_client = chromadb.PersistentClient(path=settings.chroma_persist_directory)
    collection = chroma_client.get_collection(settings.chroma_collection)

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=limit,
    )

    if not results["ids"] or not results["ids"][0]:
        return []

    product_ids = results["ids"][0]
    distances = results["distances"][0] if results.get("distances") else [0.0] * len(product_ids)

    # Convert distance to similarity score (cosine distance to similarity)
    # ChromaDB returns cosine distance, similarity = 1 - distance
    return [(pid, 1.0 - dist) for pid, dist in zip(product_ids, distances)]


def apply_personalization(
    products: list[dict[str, Any]],
    user_categories: list[tuple[str, float]],
    personalization_weight: float = 0.15,
) -> list[dict[str, Any]]:
    """Apply personalization boost based on user's category affinity.

    Boosts products in categories the user has affinity for.
    """
    if not user_categories:
        return products

    # Create category boost map
    category_boost: dict[str, float] = {}
    for cat_path, affinity in user_categories:
        # Normalize affinity score for boosting
        category_boost[cat_path] = affinity / 10.0  # Scale down

    for product in products:
        cat_path = product.get("category_path", "")
        # Check for category match (exact or parent)
        boost = 0.0
        for cat, affinity_boost in category_boost.items():
            if cat == cat_path or cat_path.startswith(cat + " > ") or cat.startswith(cat_path + " > "):
                boost = max(boost, affinity_boost * personalization_weight)
        product["personalization_boost"] = boost
        if "final_score" in product:
            product["final_score"] = product.get("final_score", 0) + boost

    return products


@router.get("/search", response_model=SearchResponse)
def search_products(
    q: str = Query(..., min_length=1, description="Search query"),
    user_id: str | None = Query(None, description="User ID for personalization"),
    size: int = Query(20, ge=1, le=100, description="Number of results"),
    use_semantic: bool = Query(True, description="Use semantic search fallback"),
) -> SearchResponse:
    """Search products with intelligent ranking.

    Ranking algorithm:
    1. BM25 text matching from Elasticsearch for initial candidate set
    2. Re-rank using weighted scoring:
       - BM25 relevance score (40%)
       - Popularity score (25%)
       - Conversion rate (15%)
       - Review score (10%)
       - In-stock boost (10%)
    3. For known users: apply category affinity personalization boost
    4. If BM25 results are weak: fall back to semantic search (ChromaDB)

    Performance target: <200ms p95 latency.
    """
    engine = get_engine()
    client = get_elasticsearch_client()
    settings = get_settings()

    user_categories: list[tuple[str, float]] = []
    is_personalized = False

    # Get user categories for personalization
    if user_id:
        with Session(engine) as session:
            user_categories = get_user_top_categories(session, user_id, limit=5)
            is_personalized = len(user_categories) > 0

    # Step 1: BM25 search in Elasticsearch
    es_query = {
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["title^3", "brand^2", "category_path^1.5", "description"],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        },
        "size": size * 2,  # Fetch more candidates for re-ranking
    }

    response = client.search(index=settings.elasticsearch_index, body=es_query)
    hits = response.get("hits", {}).get("hits", [])
    total_hits = response.get("hits", {}).get("total", {}).get("value", 0)

    # Check if BM25 results are weak (low relevance scores)
    max_bm25_score = max((hit["_score"] or 0 for hit in hits), default=0)
    used_semantic_fallback = False

    # Collect product IDs and BM25 scores
    products_data: list[dict[str, Any]] = []
    product_ids = []

    for hit in hits:
        source = hit["_source"]
        product_id = source["product_id"]
        bm25_score = hit["_score"] or 0.0
        product_ids.append(product_id)
        products_data.append({
            "product_id": product_id,
            "title": source["title"],
            "brand": source["brand"],
            "price": source["price"],
            "category_path": source["category_path"],
            "popularity_score": source.get("popularity_score", 0.0),
            "bm25_score": bm25_score,
        })

    # Step 2: Semantic search fallback if BM25 results are weak
    if use_semantic and (len(hits) < size // 2 or max_bm25_score < 5.0):
        try:
            query_embedding = get_embedding(q)
            semantic_results = semantic_search_products(query_embedding, limit=size)

            if semantic_results:
                # Merge semantic results with BM25 results
                semantic_ids = {pid for pid, _ in semantic_results}
                existing_ids = {p["product_id"] for p in products_data}

                # Add semantic-only results
                for pid, sim_score in semantic_results:
                    if pid not in existing_ids:
                        # Fetch product details from ES
                        semantic_product = fetch_products_by_ids(
                            client, [pid], index_name=settings.elasticsearch_index
                        )
                        if semantic_product:
                            p = semantic_product[0]
                            products_data.append({
                                "product_id": pid,
                                "title": p["title"],
                                "brand": p["brand"],
                                "price": p["price"],
                                "category_path": p["category_path"],
                                "popularity_score": p["popularity_score"],
                                "bm25_score": sim_score,  # Use similarity as BM25 proxy
                            })
                            product_ids.append(pid)

                used_semantic_fallback = True
        except Exception as e:
            logger.warning(f"Semantic search failed: {e}")

    # Step 3: Enrich with product stats for ranking
    if product_ids:
        with Session(engine) as session:
            product_stats = get_product_stats_batch(session, product_ids)

        # Add stats to products
        for product in products_data:
            pid = product["product_id"]
            stats = product_stats.get(pid, {})
            product["conversion_rate"] = stats.get("conversion_rate", 0.0)
            product["review_score"] = stats.get("review_score", 3.0)
            product["in_stock"] = stats.get("in_stock", True)

    # Step 4: Compute ranking scores
    for product in products_data:
        bm25_normalized = product["bm25_score"] / max_bm25_score if max_bm25_score > 0 else 0.0
        product["final_score"] = compute_ranking_score(
            bm25_score=bm25_normalized,
            popularity_score=product["popularity_score"],
            conversion_rate=product.get("conversion_rate", 0.0),
            review_score=product.get("review_score", 3.0),
            in_stock=product.get("in_stock", True),
        )

    # Step 5: Apply personalization for known users
    if user_categories:
        products_data = apply_personalization(products_data, user_categories)

    # Step 6: Sort by final score and take top N
    products_data.sort(key=lambda x: x.get("final_score", 0), reverse=True)
    top_products = products_data[:size]

    # Build response
    results = [
        SearchProductItem(
            product_id=p["product_id"],
            title=p["title"],
            brand=p["brand"],
            price=p["price"],
            category_path=p["category_path"],
            popularity_score=p["popularity_score"],
            bm25_score=p.get("bm25_score"),
            final_score=p.get("final_score"),
        )
        for p in top_products
    ]

    return SearchResponse(
        query=q,
        user_id=user_id,
        results=results,
        total_hits=total_hits,
        is_personalized=is_personalized,
        used_semantic_fallback=used_semantic_fallback,
    )
