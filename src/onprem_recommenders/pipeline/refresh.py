from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

INSERT_CHUNK = 1000


def refresh_from_select(
    connection: Connection,
    table: str,
    columns: list[str],
    select_sql: str,
    params: dict | None = None,
) -> int:
    """Replace table contents from a SELECT in one transaction.

    Readers keep the previous snapshot until this transaction commits (MVCC).
    """
    col_list = ", ".join(columns)
    staging = f"tmp_{table}"
    connection.execute(text(f"DROP TABLE IF EXISTS {staging}"))
    connection.execute(
        text(f"CREATE TEMP TABLE {staging} (LIKE {table} INCLUDING DEFAULTS EXCLUDING CONSTRAINTS)")
    )
    result = connection.execute(
        text(f"INSERT INTO {staging} ({col_list}) {select_sql}"),
        params or {},
    )
    connection.execute(text(f"TRUNCATE {table}"))
    connection.execute(text(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {staging}"))
    connection.execute(text(f"DROP TABLE {staging}"))
    return result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 0
