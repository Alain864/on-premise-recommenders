from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from ..db import replace_table_rows, session_scope
from ..models import Interaction, Product, Transaction, User


REQUIRED_SHEETS: dict[str, list[str]] = {
    "Users": ["user_id", "signup_date", "country"],
    "Products": ["product_id", "title", "brand", "price", "category_path", "description"],
    "Transactions": ["order_id", "user_id", "product_id", "timestamp"],
    "Interactions": ["event_type", "user_id", "product_id", "query_text", "timestamp"],
}


def _read_sheet(workbook_path: Path, sheet_name: str) -> pd.DataFrame:
    frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
    expected = REQUIRED_SHEETS[sheet_name]
    missing = [column for column in expected if column not in frame.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet_name}' is missing columns: {', '.join(missing)}")
    return frame[expected].copy()


def load_workbook_frames(workbook_path: Path) -> dict[str, pd.DataFrame]:
    workbook_path = Path(workbook_path)
    if not workbook_path.exists():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    users = _read_sheet(workbook_path, "Users")
    users["signup_date"] = pd.to_datetime(users["signup_date"], utc=False)

    products = _read_sheet(workbook_path, "Products")
    products["price"] = products["price"].astype(float)

    transactions = _read_sheet(workbook_path, "Transactions")
    transactions["timestamp"] = pd.to_datetime(transactions["timestamp"], utc=False)

    interactions = _read_sheet(workbook_path, "Interactions")
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


def load_source_tables(engine: Engine, workbook_path: Path) -> dict[str, int]:
    frames = load_workbook_frames(workbook_path)
    with session_scope(engine) as session:
        replace_table_rows(session, User, _records(frames["users"]))
        replace_table_rows(session, Product, _records(frames["products"]))
        replace_table_rows(session, Transaction, _records(frames["transactions"]))
        replace_table_rows(session, Interaction, _records(frames["interactions"]))

    return {table_name: len(frame) for table_name, frame in frames.items()}

