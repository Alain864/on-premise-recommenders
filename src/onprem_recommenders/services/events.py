from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from onprem_recommenders.models import Event


def write_event(
    session: Session,
    *,
    feature: str,
    event_type: str,
    user_id: str | None = None,
    product_ids: list[str] | None = None,
    query_text: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Event:
    event = Event(
        user_id=user_id,
        feature=feature,
        event_type=event_type,
        product_ids=json.dumps(product_ids) if product_ids is not None else None,
        query_text=query_text,
        metadata_json=json.dumps(metadata) if metadata is not None else None,
        timestamp=datetime.utcnow(),
    )
    session.add(event)
    session.flush()
    return event


def log_impression_task(
    engine: Engine,
    *,
    feature: str,
    user_id: str | None,
    product_ids: list[str] | None,
    query_text: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    with Session(engine) as session:
        write_event(
            session,
            feature=feature,
            event_type="impression",
            user_id=user_id,
            product_ids=product_ids,
            query_text=query_text,
            metadata=metadata,
        )
        session.commit()
