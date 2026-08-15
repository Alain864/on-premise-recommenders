from __future__ import annotations

from sqlalchemy.engine import Engine

from onprem_recommenders.pipeline.refresh import refresh_from_select

TRENDING_SQL = """
WITH ranked AS (
    SELECT
        p.product_id,
        p.category_path,
        SPLIT_PART(p.category_path, ' > ', 1) AS root_category,
        s.popularity_score AS score,
        ROW_NUMBER() OVER (
            PARTITION BY SPLIT_PART(p.category_path, ' > ', 1)
            ORDER BY s.popularity_score DESC
        ) AS rank
    FROM products p
    JOIN product_stats s ON s.product_id = p.product_id
    WHERE p.category_path IS NOT NULL AND p.category_path <> ''
),
top_roots AS (
    SELECT root_category
    FROM ranked
    GROUP BY root_category
    ORDER BY SUM(score) DESC
    LIMIT :top_categories
)
SELECT
    product_id,
    category_path,
    root_category,
    rank,
    score,
    :period AS period,
    NOW() AS computed_at
FROM ranked
WHERE root_category IN (SELECT root_category FROM top_roots)
  AND rank <= :top_n_per_category
"""


def compute_trending_products(
    engine: Engine,
    period: str = "daily",
    top_n_per_category: int = 10,
    top_categories: int = 5,
) -> int:
    with engine.begin() as connection:
        return refresh_from_select(
            connection,
            "trending_products",
            [
                "product_id",
                "category_path",
                "root_category",
                "rank",
                "score",
                "period",
                "computed_at",
            ],
            TRENDING_SQL,
            {
                "period": period,
                "top_n_per_category": top_n_per_category,
                "top_categories": top_categories,
            },
        )
