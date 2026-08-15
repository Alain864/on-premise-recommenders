from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from onprem_recommenders.deps import SessionDep
from onprem_recommenders.models import FeatureFlag
from onprem_recommenders.schemas import FeatureFlagResponse, FeatureFlagUpdate, TrendingResponse, TrendingRow, ProductItem
from onprem_recommenders.services.catalog import get_trending_rows
from onprem_recommenders.services.flags import invalidate

router = APIRouter(tags=["infrastructure"])


@router.get("/feature-flags", response_model=list[FeatureFlagResponse])
def list_flags(session: SessionDep) -> list[FeatureFlagResponse]:
    flags = session.execute(select(FeatureFlag)).scalars().all()
    return [
        FeatureFlagResponse(
            feature_name=flag.feature_name,
            variant=flag.variant,
            user_segment=flag.user_segment,
            enabled=flag.enabled,
            description=flag.description,
        )
        for flag in flags
    ]


@router.get("/feature-flags/{feature_name}", response_model=FeatureFlagResponse)
def get_flag_endpoint(feature_name: str, session: SessionDep) -> FeatureFlagResponse:
    flag = session.get(FeatureFlag, feature_name)
    if flag is None:
        raise HTTPException(status_code=404, detail=f"Unknown feature '{feature_name}'")
    return FeatureFlagResponse(
        feature_name=flag.feature_name,
        variant=flag.variant,
        user_segment=flag.user_segment,
        enabled=flag.enabled,
        description=flag.description,
    )


@router.put("/feature-flags/{feature_name}", response_model=FeatureFlagResponse)
def update_flag(feature_name: str, payload: FeatureFlagUpdate, session: SessionDep) -> FeatureFlagResponse:
    flag = session.get(FeatureFlag, feature_name)
    if flag is None:
        raise HTTPException(status_code=404, detail=f"Unknown feature '{feature_name}'")
    if payload.enabled is not None:
        flag.enabled = payload.enabled
    if payload.variant is not None:
        flag.variant = payload.variant
    if payload.description is not None:
        flag.description = payload.description
    flag.updated_at = datetime.utcnow()
    invalidate(feature_name)
    return FeatureFlagResponse(
        feature_name=flag.feature_name,
        variant=flag.variant,
        user_segment=flag.user_segment,
        enabled=flag.enabled,
        description=flag.description,
    )


@router.get("/trending", response_model=TrendingResponse)
def trending(session: SessionDep, period: str = "daily", categories: int = 3, products_per_category: int = 10) -> TrendingResponse:
    rows = get_trending_rows(
        session, period=period, categories=categories, products_per_category=products_per_category
    )
    return TrendingResponse(
        period=period,
        rows=[
            TrendingRow(
                category=category,
                products=[ProductItem(**p) for p in products],
            )
            for category, products in rows
        ],
    )
