from __future__ import annotations

from collections import Counter
from itertools import combinations

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from ..db import replace_table_rows, session_scope
from ..models import CoPurchasePair, CoViewPair, ProductStats, UserCategoryAffinity


def _read_tables(engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    products = pd.read_sql_table("products", engine)
    transactions = pd.read_sql_table("transactions", engine)
    interactions = pd.read_sql_table("interactions", engine)

    if not transactions.empty:
        transactions["timestamp"] = pd.to_datetime(transactions["timestamp"], utc=False)
    if not interactions.empty:
        interactions["timestamp"] = pd.to_datetime(interactions["timestamp"], utc=False)

    return products, transactions, interactions


def _records(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []

    cleaned = frame.replace({np.nan: None})
    records = cleaned.to_dict(orient="records")
    for record in records:
        for key, value in list(record.items()):
            if hasattr(value, "to_pydatetime"):
                record[key] = value.to_pydatetime()
    return records


def build_user_category_affinity(products: pd.DataFrame, transactions: pd.DataFrame, interactions: pd.DataFrame) -> pd.DataFrame:
    purchases = transactions.merge(
        products[["product_id", "category_path"]],
        on="product_id",
        how="left",
    )
    purchases = (
        purchases.groupby(["user_id", "category_path"], dropna=False)
        .agg(
            purchase_count=("product_id", "count"),
            last_purchase_at=("timestamp", "max"),
        )
        .reset_index()
    )

    interaction_signals = interactions[interactions["event_type"].isin(["page_view", "add_to_cart"])].merge(
        products[["product_id", "category_path"]],
        on="product_id",
        how="left",
    )

    views = (
        interaction_signals[interaction_signals["event_type"] == "page_view"]
        .groupby(["user_id", "category_path"], dropna=False)
        .agg(
            view_count=("product_id", "count"),
            last_view_at=("timestamp", "max"),
        )
        .reset_index()
    )

    carts = (
        interaction_signals[interaction_signals["event_type"] == "add_to_cart"]
        .groupby(["user_id", "category_path"], dropna=False)
        .agg(
            add_to_cart_count=("product_id", "count"),
            last_cart_at=("timestamp", "max"),
        )
        .reset_index()
    )

    affinity = purchases.merge(views, on=["user_id", "category_path"], how="outer").merge(
        carts,
        on=["user_id", "category_path"],
        how="outer",
    )
    if affinity.empty:
        return pd.DataFrame(
            columns=[
                "user_id",
                "category_path",
                "purchase_count",
                "view_count",
                "add_to_cart_count",
                "affinity_score",
                "last_signal_at",
            ]
        )

    for column in ["purchase_count", "view_count", "add_to_cart_count"]:
        affinity[column] = affinity[column].fillna(0).astype(int)

    affinity["last_signal_at"] = affinity[["last_purchase_at", "last_view_at", "last_cart_at"]].max(axis=1)
    affinity["affinity_score"] = (
        affinity["purchase_count"] * 3.0
        + affinity["add_to_cart_count"] * 2.0
        + affinity["view_count"] * 1.0
    )

    return affinity[
        [
            "user_id",
            "category_path",
            "purchase_count",
            "view_count",
            "add_to_cart_count",
            "affinity_score",
            "last_signal_at",
        ]
    ].sort_values(["user_id", "affinity_score", "category_path"], ascending=[True, False, True])


def build_product_stats(products: pd.DataFrame, transactions: pd.DataFrame, interactions: pd.DataFrame) -> pd.DataFrame:
    product_base = products[["product_id"]].copy()

    purchase_counts = (
        transactions.groupby("product_id")
        .agg(
            purchase_count=("order_id", "count"),
            last_purchase_at=("timestamp", "max"),
        )
        .reset_index()
    )
    view_counts = (
        interactions[interactions["event_type"] == "page_view"]
        .groupby("product_id", dropna=False)
        .agg(
            view_count=("id", "count"),
            last_view_at=("timestamp", "max"),
        )
        .reset_index()
    )
    cart_counts = (
        interactions[interactions["event_type"] == "add_to_cart"]
        .groupby("product_id", dropna=False)
        .agg(
            add_to_cart_count=("id", "count"),
            last_cart_at=("timestamp", "max"),
        )
        .reset_index()
    )

    stats = product_base.merge(purchase_counts, on="product_id", how="left").merge(
        view_counts,
        on="product_id",
        how="left",
    ).merge(
        cart_counts,
        on="product_id",
        how="left",
    )

    for column in ["purchase_count", "view_count", "add_to_cart_count"]:
        stats[column] = stats[column].fillna(0).astype(int)

    ctr_denominator = stats[["view_count", "add_to_cart_count"]].max(axis=1).replace(0, np.nan)
    conversion_denominator = stats[["view_count", "purchase_count"]].max(axis=1).replace(0, np.nan)
    stats["ctr_proxy"] = (stats["add_to_cart_count"] / ctr_denominator).fillna(0.0).round(6)
    stats["conversion_rate"] = (stats["purchase_count"] / conversion_denominator).fillna(0.0).round(6)
    stats["popularity_score"] = np.log1p(
        stats["purchase_count"] * 5 + stats["add_to_cart_count"] * 2 + stats["view_count"]
    ).round(6)
    stats["review_score"] = None
    stats["review_count"] = 0
    stats["in_stock"] = True
    stats["last_signal_at"] = stats[["last_purchase_at", "last_view_at", "last_cart_at"]].max(axis=1)

    return stats[
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
        ]
    ].sort_values("product_id")


def build_co_purchase_pairs(transactions: pd.DataFrame) -> pd.DataFrame:
    counter: Counter[tuple[str, str]] = Counter()
    for _, order_frame in transactions.groupby("order_id"):
        product_ids = sorted(order_frame["product_id"].dropna().unique().tolist())
        for pair in combinations(product_ids, 2):
            counter[pair] += 1

    rows = [
        {
            "left_product_id": left_product_id,
            "right_product_id": right_product_id,
            "pair_count": pair_count,
        }
        for (left_product_id, right_product_id), pair_count in sorted(counter.items())
    ]
    return pd.DataFrame(rows, columns=["left_product_id", "right_product_id", "pair_count"])


def build_co_view_pairs(interactions: pd.DataFrame, session_gap_minutes: int) -> pd.DataFrame:
    page_views = interactions[interactions["event_type"] == "page_view"].copy()
    if page_views.empty:
        return pd.DataFrame(columns=["left_product_id", "right_product_id", "pair_count"])

    page_views = page_views.sort_values(["user_id", "timestamp"])
    page_views["previous_timestamp"] = page_views.groupby("user_id")["timestamp"].shift()
    page_views["gap_minutes"] = (
        (page_views["timestamp"] - page_views["previous_timestamp"]).dt.total_seconds() / 60.0
    )
    page_views["session_break"] = (
        page_views["previous_timestamp"].isna() | (page_views["gap_minutes"] > session_gap_minutes)
    )
    page_views["session_id"] = page_views.groupby("user_id")["session_break"].cumsum()

    counter: Counter[tuple[str, str]] = Counter()
    for _, session_frame in page_views.groupby(["user_id", "session_id"]):
        product_ids = sorted(session_frame["product_id"].dropna().unique().tolist())
        for pair in combinations(product_ids, 2):
            counter[pair] += 1

    rows = [
        {
            "left_product_id": left_product_id,
            "right_product_id": right_product_id,
            "pair_count": pair_count,
        }
        for (left_product_id, right_product_id), pair_count in sorted(counter.items())
    ]
    return pd.DataFrame(rows, columns=["left_product_id", "right_product_id", "pair_count"])


def materialize_derived_tables(engine: Engine, session_gap_minutes: int) -> dict[str, int]:
    products, transactions, interactions = _read_tables(engine)

    user_category_affinity = build_user_category_affinity(products, transactions, interactions)
    product_stats = build_product_stats(products, transactions, interactions)
    co_purchase_pairs = build_co_purchase_pairs(transactions)
    co_view_pairs = build_co_view_pairs(interactions, session_gap_minutes=session_gap_minutes)

    with session_scope(engine) as session:
        replace_table_rows(session, UserCategoryAffinity, _records(user_category_affinity))
        replace_table_rows(session, ProductStats, _records(product_stats))
        replace_table_rows(session, CoPurchasePair, _records(co_purchase_pairs))
        replace_table_rows(session, CoViewPair, _records(co_view_pairs))

    return {
        "user_category_affinity": len(user_category_affinity),
        "product_stats": len(product_stats),
        "co_purchase_pairs": len(co_purchase_pairs),
        "co_view_pairs": len(co_view_pairs),
    }
