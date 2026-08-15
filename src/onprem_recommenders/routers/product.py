from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.deps import EsDep, SessionDep, SettingsDep
from onprem_recommenders.models import CoPurchasePair, CoViewPair, Product
from onprem_recommenders.schemas import ProductDetail, ProductItem, ProductPageResponse
from onprem_recommenders.services.catalog import hydrate_products
from onprem_recommenders.services.events import log_impression_task
from onprem_recommenders.services.flags import is_enabled, variant_name
from onprem_recommenders.services.search import fetch_products_by_category, get_product_source

router = APIRouter(prefix="/recommendations", tags=["recommendations"])


def _pair_products(session: Session, product_id: str, model: type, limit: int) -> list[tuple[str, int]]:
    left = session.execute(
        select(model).where(model.left_product_id == product_id).order_by(model.pair_count.desc()).limit(limit)
    ).scalars().all()
    right = session.execute(
        select(model).where(model.right_product_id == product_id).order_by(model.pair_count.desc()).limit(limit)
    ).scalars().all()
    merged: dict[str, int] = {}
    for row in left:
        merged[row.right_product_id] = max(merged.get(row.right_product_id, 0), row.pair_count)
    for row in right:
        merged[row.left_product_id] = max(merged.get(row.left_product_id, 0), row.pair_count)
    return sorted(merged.items(), key=lambda item: item[1], reverse=True)[:limit]


def _category_fallback(
    session: Session,
    es,
    product_id: str,
    limit: int,
    index_name: str,
) -> list[dict]:
    product = session.get(Product, product_id)
    category = product.category_path if product else None
    if not category:
        source = get_product_source(es, product_id, index_name)
        category = source["category_path"] if source else None
    if not category:
        return []
    return fetch_products_by_category(
        es, category, excluded_ids={product_id}, size=limit, index_name=index_name
    )


def _recommend(
    request: Request,
    background_tasks: BackgroundTasks,
    session: Session,
    es,
    settings,
    product_id: str,
    limit: int,
    model: type,
    recommendation_type: str,
) -> ProductPageResponse:
    if session.get(Product, product_id) is None and get_product_source(es, product_id, settings.elasticsearch_index) is None:
        raise HTTPException(status_code=404, detail=f"Product '{product_id}' not found")

    variant = variant_name(session, "product_page")
    use_collab = is_enabled(session, "product_page")
    fallback = True
    products: list[dict] = []

    if use_collab:
        pairs = _pair_products(session, product_id, model, limit)
        if pairs:
            products = hydrate_products(session, [pid for pid, _ in pairs])
            fallback = False

    if not products:
        products = _category_fallback(session, es, product_id, limit, settings.elasticsearch_index)
        fallback = True

    response = ProductPageResponse(
        product_id=product_id,
        recommendations=[ProductItem(**p) for p in products[:limit]],
        recommendation_type=recommendation_type,
        fallback=fallback,
        variant=variant,
    )
    background_tasks.add_task(
        log_impression_task,
        request.app.state.engine,
        feature="product_page",
        user_id=None,
        product_ids=[item.product_id for item in response.recommendations],
        metadata={"variant": variant, "type": recommendation_type, "fallback": fallback},
    )
    return response


@router.get("/product/{product_id}", response_model=ProductDetail)
def product_detail(product_id: str, session: SessionDep, es: EsDep, settings: SettingsDep) -> ProductDetail:
    product = session.get(Product, product_id)
    if product is not None:
        hydrated = hydrate_products(session, [product_id])
        row = hydrated[0] if hydrated else None
        if row:
            return ProductDetail(**row)
    source = get_product_source(es, product_id, settings.elasticsearch_index)
    if source is None:
        raise HTTPException(status_code=404, detail=f"Product '{product_id}' not found")
    return ProductDetail(**source)


@router.get("/product/{product_id}/frequently-bought-together", response_model=ProductPageResponse)
def frequently_bought_together(
    request: Request,
    background_tasks: BackgroundTasks,
    session: SessionDep,
    es: EsDep,
    settings: SettingsDep,
    product_id: str,
    limit: int = Query(default=10, ge=1, le=50),
) -> ProductPageResponse:
    return _recommend(
        request, background_tasks, session, es, settings, product_id, limit, CoPurchasePair, "frequently_bought_together"
    )


@router.get("/product/{product_id}/customers-also-viewed", response_model=ProductPageResponse)
def customers_also_viewed(
    request: Request,
    background_tasks: BackgroundTasks,
    session: SessionDep,
    es: EsDep,
    settings: SettingsDep,
    product_id: str,
    limit: int = Query(default=10, ge=1, le=50),
) -> ProductPageResponse:
    return _recommend(
        request, background_tasks, session, es, settings, product_id, limit, CoViewPair, "customers_also_viewed"
    )
