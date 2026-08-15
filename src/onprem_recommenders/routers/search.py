from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Query, Request
from openai import OpenAI
from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.deps import EsDep, SessionDep, SettingsDep
from onprem_recommenders.models import ProductStats, UserCategoryAffinity
from onprem_recommenders.ranking import apply_personalization, compute_ranking_score
from onprem_recommenders.schemas import SearchProductItem, SearchResponse
from onprem_recommenders.services.events import log_impression_task
from onprem_recommenders.services.flags import is_enabled, variant_name
from onprem_recommenders.services.search import bm25_search, fetch_products_by_ids
from onprem_recommenders.services.vectors import similar_products

router = APIRouter(prefix="/recommendations", tags=["recommendations"])


def _user_categories(session: Session, user_id: str, limit: int = 5) -> list[tuple[str, float]]:
    rows = session.execute(
        select(UserCategoryAffinity)
        .where(UserCategoryAffinity.user_id == user_id)
        .order_by(UserCategoryAffinity.affinity_score.desc())
        .limit(limit)
    ).scalars().all()
    return [(row.category_path, row.affinity_score) for row in rows]


@router.get("/search", response_model=SearchResponse)
def search_products(
    request: Request,
    background_tasks: BackgroundTasks,
    session: SessionDep,
    es: EsDep,
    settings: SettingsDep,
    q: str = Query(..., min_length=1),
    user_id: str | None = Query(default=None),
    size: int = Query(default=20, ge=1, le=100),
    use_semantic: bool = Query(default=True),
) -> SearchResponse:
    variant = variant_name(session, "search_ranking")
    ranking_enabled = is_enabled(session, "search_ranking")

    user_categories: list[tuple[str, float]] = []
    if user_id and ranking_enabled:
        user_categories = _user_categories(session, user_id)

    products, total_hits, max_bm25 = bm25_search(
        es, q, size=size * 2 if ranking_enabled else size, index_name=settings.elasticsearch_index
    )
    used_semantic = False

    if ranking_enabled and use_semantic and settings.openai_api_key and (
        len(products) < size // 2 or max_bm25 < 5.0
    ):
        try:
            client = OpenAI(api_key=settings.openai_api_key)
            embedding = client.embeddings.create(
                input=q, model=settings.openai_embedding_model
            ).data[0].embedding
            semantic_hits = similar_products(session, embedding, limit=size)
            existing = {item["product_id"] for item in products}
            new_ids = [pid for pid, _score in semantic_hits if pid not in existing]
            extras = fetch_products_by_ids(es, new_ids, index_name=settings.elasticsearch_index)
            sim_by_id = dict(semantic_hits)
            for extra in extras:
                extra["bm25_score"] = sim_by_id.get(extra["product_id"], 0.0)
                products.append(extra)
            used_semantic = bool(extras) or bool(semantic_hits)
        except Exception:
            used_semantic = False

    if ranking_enabled:
        stats_rows = session.execute(
            select(ProductStats).where(ProductStats.product_id.in_([p["product_id"] for p in products]))
        ).scalars().all() if products else []
        stats = {row.product_id: row for row in stats_rows}
        for product in products:
            row = stats.get(product["product_id"])
            bm25_normalized = (product.get("bm25_score") or 0.0) / max_bm25 if max_bm25 > 0 else 0.0
            product["final_score"] = compute_ranking_score(
                bm25_score=bm25_normalized,
                popularity_score=product.get("popularity_score", 0.0),
                conversion_rate=float(row.conversion_rate) if row else 0.0,
                review_score=float(row.review_score) if row and row.review_score else 3.0,
                in_stock=bool(row.in_stock) if row else True,
            )
        if user_categories:
            apply_personalization(products, user_categories)
        products.sort(key=lambda item: item.get("final_score", 0), reverse=True)
    else:
        for product in products:
            product["final_score"] = product.get("bm25_score", 0.0)

    top = products[:size]
    response = SearchResponse(
        query=q,
        user_id=user_id,
        results=[SearchProductItem(**item) for item in top],
        total_hits=total_hits,
        is_personalized=bool(user_categories),
        used_semantic_fallback=used_semantic,
        variant=variant,
    )
    background_tasks.add_task(
        log_impression_task,
        request.app.state.engine,
        feature="search",
        user_id=user_id,
        product_ids=[item.product_id for item in response.results],
        query_text=q,
        metadata={"variant": variant, "semantic": used_semantic},
    )
    return response
