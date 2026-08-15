from __future__ import annotations

from sqlalchemy.engine import Engine

from onprem_recommenders.pipeline.refresh import refresh_from_select

AFFINITY_SQL = """
SELECT
    COALESCE(pu.user_id, v.user_id, c.user_id) AS user_id,
    COALESCE(pu.category_path, v.category_path, c.category_path) AS category_path,
    COALESCE(pu.purchase_count, 0) AS purchase_count,
    COALESCE(v.view_count, 0) AS view_count,
    COALESCE(c.add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(pu.purchase_count, 0) * 3.0
        + COALESCE(c.add_to_cart_count, 0) * 2.0
        + COALESCE(v.view_count, 0) * 1.0 AS affinity_score,
    GREATEST(pu.last_purchase_at, v.last_view_at, c.last_cart_at) AS last_signal_at
FROM (
    SELECT t.user_id, p.category_path, COUNT(*) AS purchase_count, MAX(t.timestamp) AS last_purchase_at
    FROM transactions t
    JOIN products p ON p.product_id = t.product_id
    GROUP BY t.user_id, p.category_path
) pu
FULL OUTER JOIN (
    SELECT i.user_id, p.category_path, COUNT(*) AS view_count, MAX(i.timestamp) AS last_view_at
    FROM interactions i
    JOIN products p ON p.product_id = i.product_id
    WHERE i.event_type = 'page_view'
    GROUP BY i.user_id, p.category_path
) v ON pu.user_id = v.user_id AND pu.category_path = v.category_path
FULL OUTER JOIN (
    SELECT i.user_id, p.category_path, COUNT(*) AS add_to_cart_count, MAX(i.timestamp) AS last_cart_at
    FROM interactions i
    JOIN products p ON p.product_id = i.product_id
    WHERE i.event_type = 'add_to_cart'
    GROUP BY i.user_id, p.category_path
) c ON COALESCE(pu.user_id, v.user_id) = c.user_id
   AND COALESCE(pu.category_path, v.category_path) = c.category_path
WHERE COALESCE(pu.category_path, v.category_path, c.category_path) IS NOT NULL
  AND COALESCE(pu.category_path, v.category_path, c.category_path) <> ''
"""

STATS_SQL = """
SELECT
    p.product_id,
    COALESCE(v.view_count, 0) AS view_count,
    COALESCE(c.add_to_cart_count, 0) AS add_to_cart_count,
    COALESCE(t.purchase_count, 0) AS purchase_count,
    CASE
        WHEN GREATEST(COALESCE(v.view_count, 0), COALESCE(c.add_to_cart_count, 0)) = 0 THEN 0
        ELSE ROUND(
            (COALESCE(c.add_to_cart_count, 0)::numeric
             / GREATEST(COALESCE(v.view_count, 0), COALESCE(c.add_to_cart_count, 0)))::numeric,
            6
        )
    END AS ctr_proxy,
    CASE
        WHEN GREATEST(COALESCE(v.view_count, 0), COALESCE(t.purchase_count, 0)) = 0 THEN 0
        ELSE ROUND(
            (COALESCE(t.purchase_count, 0)::numeric
             / GREATEST(COALESCE(v.view_count, 0), COALESCE(t.purchase_count, 0)))::numeric,
            6
        )
    END AS conversion_rate,
    NULL::float AS review_score,
    0 AS review_count,
    TRUE AS in_stock,
    ROUND(LN(1 + COALESCE(t.purchase_count, 0) * 5 + COALESCE(c.add_to_cart_count, 0) * 2 + COALESCE(v.view_count, 0))::numeric, 6) AS popularity_score,
    GREATEST(t.last_purchase_at, v.last_view_at, c.last_cart_at) AS last_signal_at
FROM products p
LEFT JOIN (
    SELECT product_id, COUNT(*) AS purchase_count, MAX(timestamp) AS last_purchase_at
    FROM transactions
    GROUP BY product_id
) t ON t.product_id = p.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS view_count, MAX(timestamp) AS last_view_at
    FROM interactions
    WHERE event_type = 'page_view'
    GROUP BY product_id
) v ON v.product_id = p.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS add_to_cart_count, MAX(timestamp) AS last_cart_at
    FROM interactions
    WHERE event_type = 'add_to_cart'
    GROUP BY product_id
) c ON c.product_id = p.product_id
"""

CO_PURCHASE_SQL = """
SELECT a.product_id AS left_product_id, b.product_id AS right_product_id, COUNT(*)::int AS pair_count
FROM transactions a
JOIN transactions b
  ON a.order_id = b.order_id
 AND a.product_id < b.product_id
GROUP BY a.product_id, b.product_id
"""

CO_VIEW_SQL = """
WITH views AS (
    SELECT
        user_id,
        product_id,
        timestamp,
        LAG(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) AS prev_ts
    FROM interactions
    WHERE event_type = 'page_view' AND product_id IS NOT NULL
),
sessioned AS (
    SELECT
        user_id,
        product_id,
        SUM(
            CASE
                WHEN prev_ts IS NULL THEN 1
                WHEN EXTRACT(EPOCH FROM (timestamp - prev_ts)) / 60.0 > :session_gap_minutes THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY user_id ORDER BY timestamp) AS session_id
    FROM views
),
distinct_in_session AS (
    SELECT DISTINCT user_id, session_id, product_id
    FROM sessioned
)
SELECT a.product_id AS left_product_id, b.product_id AS right_product_id, COUNT(*)::int AS pair_count
FROM distinct_in_session a
JOIN distinct_in_session b
  ON a.user_id = b.user_id
 AND a.session_id = b.session_id
 AND a.product_id < b.product_id
GROUP BY a.product_id, b.product_id
"""


def materialize_derived_tables(engine: Engine, session_gap_minutes: int) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.begin() as connection:
        counts["user_category_affinity"] = refresh_from_select(
            connection,
            "user_category_affinity",
            [
                "user_id",
                "category_path",
                "purchase_count",
                "view_count",
                "add_to_cart_count",
                "affinity_score",
                "last_signal_at",
            ],
            AFFINITY_SQL,
        )
        counts["product_stats"] = refresh_from_select(
            connection,
            "product_stats",
            [
                "product_id",
                "view_count",
                "add_to_cart_count",
                "purchase_count",
                "ctr_proxy",
                "conversion_rate",
                "review_score",
                "review_count",
                "in_stock",
                "popularity_score",
                "last_signal_at",
            ],
            STATS_SQL,
        )
        counts["co_purchase_pairs"] = refresh_from_select(
            connection,
            "co_purchase_pairs",
            ["left_product_id", "right_product_id", "pair_count"],
            CO_PURCHASE_SQL,
        )
        counts["co_view_pairs"] = refresh_from_select(
            connection,
            "co_view_pairs",
            ["left_product_id", "right_product_id", "pair_count"],
            CO_VIEW_SQL,
            {"session_gap_minutes": session_gap_minutes},
        )
    return counts
