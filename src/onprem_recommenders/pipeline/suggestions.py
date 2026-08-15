from __future__ import annotations

from sqlalchemy.engine import Engine

from onprem_recommenders.pipeline.refresh import refresh_from_select

SUGGESTIONS_SQL = """
SELECT query_text, ''::varchar AS category_path, COUNT(*)::int AS frequency, NOW() AS last_updated
FROM interactions
WHERE event_type = 'search' AND query_text IS NOT NULL AND BTRIM(query_text) <> ''
GROUP BY query_text
UNION ALL
SELECT i.query_text, p.category_path, COUNT(*)::int AS frequency, NOW() AS last_updated
FROM interactions i
JOIN products p ON p.product_id = i.product_id
WHERE i.event_type = 'search'
  AND i.query_text IS NOT NULL
  AND BTRIM(i.query_text) <> ''
  AND p.category_path <> ''
GROUP BY i.query_text, p.category_path
"""


def materialize_query_suggestions(engine: Engine) -> int:
    with engine.begin() as connection:
        return refresh_from_select(
            connection,
            "query_suggestions",
            ["query_text", "category_path", "frequency", "last_updated"],
            SUGGESTIONS_SQL,
        )
