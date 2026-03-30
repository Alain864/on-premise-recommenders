"""ETL for building query suggestions from search interactions."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from ..db import replace_table_rows, session_scope
from ..models import QuerySuggestion


def build_query_suggestions(
    interactions: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """Build query suggestions from search interactions.

    Creates two types of suggestions:
    1. Global query frequency (category_path is NULL)
    2. Category-specific query frequency (category_path is set)

    The global suggestions are used for anonymous users.
    The category-specific suggestions enable personalized autocomplete.
    """
    # Filter for search events with query text
    searches = interactions[
        (interactions["event_type"] == "search") &
        (interactions["query_text"].notna())
    ].copy()

    if searches.empty:
        return pd.DataFrame(columns=[
            "query_text",
            "frequency",
            "category_path",
            "last_updated",
        ])

    # Global query frequency
    global_queries = (
        searches.groupby("query_text")
        .agg(frequency=("user_id", "count"))
        .reset_index()
    )
    global_queries["category_path"] = None
    global_queries["last_updated"] = datetime.utcnow()

    # Category-specific query frequency
    # Join with products to get categories for product searches
    searches_with_product = searches[searches["product_id"].notna()].merge(
        products[["product_id", "category_path"]],
        on="product_id",
        how="left",
    )

    # Also join with interactions that have implicit category from context
    # For searches without a product, we can infer category from the user's
    # subsequent views or purchases in the same session

    category_queries = (
        searches_with_product[searches_with_product["category_path"].notna()]
        .groupby(["query_text", "category_path"])
        .agg(frequency=("user_id", "count"))
        .reset_index()
    )
    category_queries["last_updated"] = datetime.utcnow()

    # Combine global and category-specific
    combined = pd.concat([
        global_queries[["query_text", "frequency", "category_path", "last_updated"]],
        category_queries[["query_text", "frequency", "category_path", "last_updated"]],
    ], ignore_index=True)

    # Sort by frequency descending
    combined = combined.sort_values(["frequency", "query_text"], ascending=[False, True])

    return combined


def materialize_query_suggestions(engine: Engine) -> int:
    """Build and persist query suggestions from interactions."""
    interactions = pd.read_sql_table("interactions", engine)
    products = pd.read_sql_table("products", engine)

    suggestions = build_query_suggestions(interactions, products)

    # Convert to records for database insertion
    records = []
    for _, row in suggestions.iterrows():
        records.append({
            "query_text": row["query_text"],
            "frequency": int(row["frequency"]),
            "category_path": row["category_path"],
            "last_updated": row["last_updated"],
        })

    with session_scope(engine) as session:
        replace_table_rows(session, QuerySuggestion, records)

    return len(suggestions)