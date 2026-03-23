from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, delete, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .models import Base


def create_db_engine(database_url: str) -> Engine:
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(database_url, future=True, connect_args=connect_args)


def init_db(engine: Engine) -> None:
    Base.metadata.create_all(engine)


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


def replace_table_rows(session: Session, model: type[Base], rows: list[dict]) -> None:
    session.execute(delete(model))
    if rows:
        session.execute(insert(model), rows)

