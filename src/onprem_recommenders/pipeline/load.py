from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import case, delete, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from onprem_recommenders.models import Interaction, Product, Transaction, User

REQUIRED_TABLES: dict[str, list[str]] = {
    "users": ["user_id", "signup_date", "country"],
    "products": ["product_id", "title", "brand", "price", "category_path", "description"],
    "transactions": ["order_id", "user_id", "product_id", "timestamp"],
    "interactions": ["event_type", "user_id", "product_id", "query_text", "timestamp"],
}

INSERT_CHUNK = 2000


def content_hash(title: str, category_path: str, description: str) -> str:
    payload = f"{title.strip()} | {category_path.strip()} | {description.strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _join_nested(value: Any) -> str:
    if isinstance(value, (list, np.ndarray)):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return " > ".join(parts) if parts else ""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def _join_description(value: Any) -> str:
    if isinstance(value, (list, np.ndarray)):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return " ".join(parts)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


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
    products["category_path"] = products["category_path"].map(_join_nested)
    products["description"] = products["description"].map(_join_description)
    products["embedding_hash"] = [
        content_hash(str(row.title), str(row.category_path), str(row.description))
        for row in products.itertuples(index=False)
    ]

    transactions = _read_parquet_file(source_paths["transactions"], "transactions")
    transactions["timestamp"] = pd.to_datetime(transactions["timestamp"], utc=False)

    interactions = _read_parquet_file(source_paths["interactions"], "interactions")
    interactions["timestamp"] = pd.to_datetime(interactions["timestamp"], utc=False)
    interactions["product_id"] = interactions["product_id"].where(
        interactions["product_id"].notna() & (interactions["product_id"].astype(str) != ""),
        None,
    )
    interactions["query_text"] = interactions["query_text"].where(
        interactions["query_text"].notna() & (interactions["query_text"].astype(str).str.strip() != ""),
        None,
    )

    return {
        "users": users,
        "products": products,
        "transactions": transactions,
        "interactions": interactions,
    }


def _to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    records = frame.to_dict(orient="records")
    for record in records:
        for key, value in list(record.items()):
            if isinstance(value, np.ndarray):
                continue
            try:
                if pd.isna(value):
                    record[key] = None
                elif hasattr(value, "to_pydatetime"):
                    record[key] = value.to_pydatetime()
            except (ValueError, TypeError):
                pass
    return records


def _bulk_insert(session: Session, model: type, records: list[dict[str, Any]]) -> None:
    for start in range(0, len(records), INSERT_CHUNK):
        session.execute(insert(model), records[start : start + INSERT_CHUNK])


def load_source_tables(engine: Engine, source_paths: dict[str, Path]) -> dict[str, int]:
    frames = load_parquet_frames(source_paths)
    users = _to_records(frames["users"])
    products = _to_records(frames["products"])
    transactions = _to_records(frames["transactions"])
    interactions = _to_records(frames["interactions"])

    with Session(engine) as session:
        session.execute(text("TRUNCATE interactions, transactions, users"))
        _bulk_insert(session, User, users)
        _upsert_products(session, products)
        incoming_ids = [row["product_id"] for row in products]
        if incoming_ids:
            session.execute(delete(Product).where(Product.product_id.not_in(incoming_ids)))
        _bulk_insert(session, Transaction, transactions)
        _bulk_insert(session, Interaction, interactions)
        session.commit()

    return {name: len(frame) for name, frame in frames.items()}


def _upsert_products(session: Session, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    for start in range(0, len(records), INSERT_CHUNK):
        chunk = records[start : start + INSERT_CHUNK]
        stmt = pg_insert(Product).values(chunk)
        excluded = stmt.excluded
        stmt = stmt.on_conflict_do_update(
            index_elements=[Product.product_id],
            set_={
                "title": excluded.title,
                "brand": excluded.brand,
                "price": excluded.price,
                "category_path": excluded.category_path,
                "description": excluded.description,
                "embedding": case(
                    (Product.embedding_hash == excluded.embedding_hash, Product.embedding),
                    else_=None,
                ),
                "embedding_hash": excluded.embedding_hash,
            },
        )
        session.execute(stmt)
