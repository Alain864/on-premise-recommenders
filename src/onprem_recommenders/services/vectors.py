from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.models import Product


def similar_products(session: Session, query_embedding: list[float], limit: int = 50) -> list[tuple[str, float]]:
    result = session.execute(
        select(Product.product_id, (1 - Product.embedding.cosine_distance(query_embedding)).label("similarity"))
        .where(Product.embedding.is_not(None))
        .order_by(Product.embedding.cosine_distance(query_embedding))
        .limit(limit)
    )
    return [(row.product_id, float(row.similarity)) for row in result]
