from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy.engine import Engine

from ..db import replace_table_rows, session_scope
from ..models import Interaction, Product, Transaction, User


REQUIRED_TABLES: dict[str, list[str]] = {
    "users": ["user_id", "signup_date", "country"],
    "products": ["product_id", "title", "brand", "price", "category_path", "description"],
    "transactions": ["order_id", "user_id", "product_id", "timestamp"],
    "interactions": ["event_type", "user_id", "product_id", "query_text", "timestamp"],
}


def _read_parquet_file(path: Path, table_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing parquet file for '{table_name}': {path}")

    frame = pd.read_parquet(path)
    expected = REQUIRED_TABLES[table_name]
    missing = [column for column in expected if column not in frame.columns]
    if missing:
        raise ValueError(f"Parquet file '{path}' is missing columns: {', '.join(missing)}")
    return frame[expected].copy()


def load_parquet_frames(source_paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    users = _read_parquet_file(source_paths["users"], "users")
    users["signup_date"] = pd.to_datetime(users["signup_date"], utc=False)

    products = _read_parquet_file(source_paths["products"], "products")
    products["price"] = products["price"].astype(float)
    # Convert category_path from numpy arrays to delimited strings
    products["category_path"] = products["category_path"].apply(
        lambda x: " > ".join(x) if isinstance(x, (list, np.ndarray)) else x
    )

    transactions = _read_parquet_file(source_paths["transactions"], "transactions")
    transactions["timestamp"] = pd.to_datetime(transactions["timestamp"], utc=False)

    interactions = _read_parquet_file(source_paths["interactions"], "interactions")
    interactions["timestamp"] = pd.to_datetime(interactions["timestamp"], utc=False)
    interactions["product_id"] = interactions["product_id"].where(interactions["product_id"].notna(), None)
    interactions["query_text"] = interactions["query_text"].where(interactions["query_text"].notna(), None)

    return {
        "users": users,
        "products": products,
        "transactions": transactions,
        "interactions": interactions,
    }


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records = frame.to_dict(orient="records")
    for record in records:
        for key, value in list(record.items()):
            if pd.isna(value):
                record[key] = None
            elif hasattr(value, "to_pydatetime"):
                record[key] = value.to_pydatetime()
    return records


def load_source_tables(engine: Engine, source_paths: dict[str, Path]) -> dict[str, int]:
    frames = load_parquet_frames(source_paths)
    with session_scope(engine) as session:
        replace_table_rows(session, User, _records(frames["users"]))
        replace_table_rows(session, Product, _records(frames["products"]))
        replace_table_rows(session, Transaction, _records(frames["transactions"]))
        replace_table_rows(session, Interaction, _records(frames["interactions"]))

    return {table_name: len(frame) for table_name, frame in frames.items()}
