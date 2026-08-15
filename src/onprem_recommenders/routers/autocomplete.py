from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Query, Request
from sqlalchemy import select

from onprem_recommenders.deps import AutocompleteDep, SessionDep
from onprem_recommenders.models import UserCategoryAffinity
from onprem_recommenders.schemas import AutocompleteResponse
from onprem_recommenders.services.events import log_impression_task
from onprem_recommenders.services.flags import is_enabled, variant_name

router = APIRouter(prefix="/autocomplete", tags=["autocomplete"])


@router.get("/suggest", response_model=AutocompleteResponse)
def suggest(
    request: Request,
    background_tasks: BackgroundTasks,
    session: SessionDep,
    index: AutocompleteDep,
    prefix: Annotated[str, Query(min_length=1, max_length=100)],
    user_id: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=20)] = 10,
) -> AutocompleteResponse:
    variant = variant_name(session, "autocomplete")
    categories: list[str] = []
    if user_id and is_enabled(session, "autocomplete"):
        rows = session.execute(
            select(UserCategoryAffinity)
            .where(UserCategoryAffinity.user_id == user_id)
            .order_by(UserCategoryAffinity.affinity_score.desc())
            .limit(5)
        ).scalars().all()
        categories = [row.category_path for row in rows]

    suggestions, is_personalized = index.suggest(
        prefix, user_categories=categories or None, limit=limit
    )
    response = AutocompleteResponse(
        prefix=prefix,
        suggestions=suggestions,
        is_personalized=is_personalized,
        user_id=user_id if is_personalized else None,
        variant=variant,
    )
    background_tasks.add_task(
        log_impression_task,
        request.app.state.engine,
        feature="autocomplete",
        user_id=user_id,
        product_ids=None,
        query_text=prefix,
        metadata={
            "variant": variant,
            "suggestions": [item.query_text for item in suggestions],
        },
    )
    return response
