from __future__ import annotations

from fastapi import APIRouter

from onprem_recommenders.deps import SessionDep
from onprem_recommenders.schemas import EventRequest, EventResponse
from onprem_recommenders.services.events import write_event

router = APIRouter(tags=["infrastructure"])


@router.post("/events", response_model=EventResponse)
def create_event(payload: EventRequest, session: SessionDep) -> EventResponse:
    event = write_event(
        session,
        feature=payload.feature,
        event_type=payload.event_type,
        user_id=payload.user_id,
        product_ids=payload.product_ids,
        query_text=payload.query_text,
        metadata=payload.metadata,
    )
    return EventResponse(success=True, event_id=event.id, message="logged")
