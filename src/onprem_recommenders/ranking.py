"""Pure ranking helpers. Same weights and formulas as the original POC."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def is_session_break(
    previous: datetime | None,
    current: datetime,
    gap_minutes: int = 30,
) -> bool:
    if previous is None:
        return True
    return (current - previous).total_seconds() / 60.0 > gap_minutes

DEFAULT_WEIGHTS = {
    "bm25": 0.40,
    "popularity": 0.25,
    "conversion": 0.15,
    "review": 0.10,
    "in_stock": 0.10,
}

AFFINITY_PURCHASE_WEIGHT = 3.0
AFFINITY_CART_WEIGHT = 2.0
AFFINITY_VIEW_WEIGHT = 1.0

POPULARITY_PURCHASE_WEIGHT = 5
POPULARITY_CART_WEIGHT = 2
POPULARITY_VIEW_WEIGHT = 1


def affinity_score(purchase_count: int, add_to_cart_count: int, view_count: int) -> float:
    return (
        purchase_count * AFFINITY_PURCHASE_WEIGHT
        + add_to_cart_count * AFFINITY_CART_WEIGHT
        + view_count * AFFINITY_VIEW_WEIGHT
    )


def compute_ranking_score(
    bm25_score: float,
    popularity_score: float,
    conversion_rate: float,
    review_score: float,
    in_stock: bool,
    weights: dict[str, float] | None = None,
) -> float:
    """Weighted combination. BM25 is expected already normalized to 0-1."""
    if weights is None:
        weights = DEFAULT_WEIGHTS

    popularity_normalized = min(popularity_score / 5.0, 1.0)
    review_normalized = (review_score - 1.0) / 4.0 if review_score else 0.5
    in_stock_boost = 1.0 if in_stock else 0.5

    return (
        weights["bm25"] * bm25_score
        + weights["popularity"] * popularity_normalized
        + weights["conversion"] * conversion_rate
        + weights["review"] * review_normalized
        + weights["in_stock"] * in_stock_boost
    )


def apply_personalization(
    products: list[dict[str, Any]],
    user_categories: list[tuple[str, float]],
    personalization_weight: float = 0.15,
) -> list[dict[str, Any]]:
    if not user_categories:
        return products

    category_boost: dict[str, float] = {}
    for cat_path, affinity in user_categories:
        category_boost[cat_path] = affinity / 10.0

    for product in products:
        cat_path = product.get("category_path", "")
        boost = 0.0
        for cat, affinity_boost in category_boost.items():
            if cat == cat_path or cat_path.startswith(cat + " > ") or cat.startswith(cat_path + " > "):
                boost = max(boost, affinity_boost * personalization_weight)
        product["personalization_boost"] = boost
        if "final_score" in product:
            product["final_score"] = product.get("final_score", 0) + boost

    return products


def category_row_label(category_path: str, *, trending: bool = False) -> str:
    parts = [part for part in category_path.split(" > ") if part]
    leaf = parts[-1] if parts else category_path
    root = parts[0] if parts else category_path
    prefix = "Trending in" if trending else "Recommended in"
    if leaf != root:
        return f"{prefix} {leaf} ({root})"
    return f"{prefix} {leaf}"
