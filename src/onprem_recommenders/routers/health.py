from __future__ import annotations

from fastapi import APIRouter, Request
from sqlalchemy import text

from onprem_recommenders.deps import EsDep, SessionDep, SettingsDep

router = APIRouter(tags=["health"])


@router.get("/health")
def health(request: Request, session: SessionDep, es: EsDep, settings: SettingsDep) -> dict[str, str]:
    session.execute(text("SELECT 1"))
    es_ok = "unknown"
    try:
        es_ok = es.cluster.health()["status"]
    except Exception:
        es_ok = "unreachable"
    return {
        "status": "ok",
        "database": "ok",
        "elasticsearch": es_ok,
        "autocomplete_size": str(len(request.app.state.autocomplete)),
    }
