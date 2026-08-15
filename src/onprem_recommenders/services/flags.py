from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.config import get_settings
from onprem_recommenders.models import FeatureFlag

_CACHE: dict[str, tuple[FeatureFlag, datetime]] = {}


def get_flag(session: Session, feature_name: str) -> FeatureFlag | None:
    settings = get_settings()
    cached = _CACHE.get(feature_name)
    if cached is not None:
        flag, cached_at = cached
        if datetime.utcnow() - cached_at < timedelta(seconds=settings.feature_flag_cache_ttl_seconds):
            return flag

    flag = session.execute(
        select(FeatureFlag).where(FeatureFlag.feature_name == feature_name)
    ).scalars().first()
    if flag is not None:
        _CACHE[feature_name] = (flag, datetime.utcnow())
    return flag


def is_enabled(session: Session, feature_name: str) -> bool:
    flag = get_flag(session, feature_name)
    return bool(flag is None or flag.enabled)


def variant_name(session: Session, feature_name: str) -> str:
    flag = get_flag(session, feature_name)
    if flag is None:
        return "control"
    return flag.variant


def invalidate(feature_name: str | None = None) -> None:
    if feature_name is None:
        _CACHE.clear()
        return
    _CACHE.pop(feature_name, None)
