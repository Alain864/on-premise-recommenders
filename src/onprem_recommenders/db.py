from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from pgvector.psycopg import register_vector
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session


def create_db_engine(database_url: str, *, pool_size: int = 10) -> Engine:
    engine = create_engine(
        database_url,
        future=True,
        pool_size=pool_size,
        max_overflow=20,
        pool_pre_ping=True,
    )

    @event.listens_for(engine, "connect")
    def _register_vector(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        register_vector(dbapi_connection)

    return engine


@contextmanager
def session_scope(engine: Engine) -> Iterator[Session]:
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
