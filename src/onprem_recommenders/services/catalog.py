from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.models import Product, ProductStats, TrendingProduct


def get_trending_rows(
    session: Session,
    *,
    period: str = "daily",
    categories: int = 3,
    products_per_category: int = 10,
) -> list[tuple[str, list[dict[str, Any]]]]:
    roots = session.execute(
        select(TrendingProduct.root_category)
        .where(TrendingProduct.period == period)
        .distinct()
        .limit(categories)
    ).scalars().all()

    rows: list[tuple[str, list[dict[str, Any]]]] = []
    for root in roots:
        hits = session.execute(
            select(TrendingProduct, Product, ProductStats)
            .join(Product, Product.product_id == TrendingProduct.product_id)
            .outerjoin(ProductStats, ProductStats.product_id == Product.product_id)
            .where(TrendingProduct.period == period, TrendingProduct.root_category == root)
            .order_by(TrendingProduct.rank)
            .limit(products_per_category)
        ).all()
        products = [_product_dict(product, stats) for _, product, stats in hits]
        if products:
            rows.append((root, products))
    return rows


def hydrate_products(session: Session, product_ids: list[str]) -> list[dict[str, Any]]:
    if not product_ids:
        return []
    rows = session.execute(
        select(Product, ProductStats)
        .outerjoin(ProductStats, ProductStats.product_id == Product.product_id)
        .where(Product.product_id.in_(product_ids))
    ).all()
    by_id = {product.product_id: _product_dict(product, stats) for product, stats in rows}
    return [by_id[pid] for pid in product_ids if pid in by_id]


def _product_dict(product: Product, stats: ProductStats | None) -> dict[str, Any]:
    return {
        "product_id": product.product_id,
        "title": product.title,
        "brand": product.brand,
        "price": float(product.price),
        "category_path": product.category_path,
        "description": product.description,
        "popularity_score": float(stats.popularity_score) if stats else 0.0,
        "conversion_rate": float(stats.conversion_rate) if stats else 0.0,
        "review_score": float(stats.review_score) if stats and stats.review_score else 3.0,
        "in_stock": bool(stats.in_stock) if stats else True,
    }
