from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Query, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.deps import EsDep, SessionDep, SettingsDep
from onprem_recommenders.models import Transaction, UserCategoryAffinity
from onprem_recommenders.ranking import category_row_label
from onprem_recommenders.schemas import HomepageResponse, ProductItem, RecommendationRow
from onprem_recommenders.services.catalog import get_trending_rows
from onprem_recommenders.services.events import log_impression_task
from onprem_recommenders.services.flags import is_enabled, variant_name
from onprem_recommenders.services.search import fetch_products_by_category

router = APIRouter(prefix="/recommendations", tags=["recommendations"])


def _user_top_categories(session: Session, user_id: str, limit: int) -> list[tuple[str, float]]:
    rows = session.execute(
        select(UserCategoryAffinity)
        .where(UserCategoryAffinity.user_id == user_id)
        .order_by(UserCategoryAffinity.affinity_score.desc())
        .limit(limit)
    ).scalars().all()
    return [(row.category_path, row.affinity_score) for row in rows]


def _purchased_ids(session: Session, user_id: str) -> set[str]:
    rows = session.execute(select(Transaction.product_id).where(Transaction.user_id == user_id)).all()
    return {row[0] for row in rows}


def _trending_response(
    session: Session,
    user_id: str,
    rows: int,
    products_per_row: int,
    variant: str,
) -> HomepageResponse:
    trending = get_trending_rows(
        session, period="daily", categories=rows, products_per_category=products_per_row
    )
    return HomepageResponse(
        user_id=user_id,
        rows=[
            RecommendationRow(
                row_label=category_row_label(category, trending=True),
                products=[ProductItem(**{k: p[k] for k in ProductItem.model_fields}) for p in products],
            )
            for category, products in trending
        ],
        is_personalized=False,
        variant=variant,
    )


@router.get("/homepage", response_model=HomepageResponse)
def homepage(
    request: Request,
    background_tasks: BackgroundTasks,
    session: SessionDep,
    es: EsDep,
    settings: SettingsDep,
    user_id: str | None = Query(default=None),
    rows: int = Query(default=3, ge=1, le=10),
    products_per_row: int = Query(default=10, ge=1, le=50),
) -> HomepageResponse:
    variant = variant_name(session, "homepage")
    personalized_enabled = is_enabled(session, "homepage")

    if user_id is None or not personalized_enabled:
        response = _trending_response(session, user_id or "anonymous", rows, products_per_row, variant)
    else:
        categories = _user_top_categories(session, user_id, limit=rows)
        purchased = _purchased_ids(session, user_id)
        if not categories:
            response = _trending_response(session, user_id, rows, products_per_row, variant)
        else:
            rec_rows: list[RecommendationRow] = []
            for category_path, _score in categories:
                products = fetch_products_by_category(
                    es,
                    category_path,
                    excluded_ids=purchased,
                    size=products_per_row,
                    index_name=settings.elasticsearch_index,
                )
                if products:
                    rec_rows.append(
                        RecommendationRow(
                            row_label=category_row_label(category_path),
                            products=[ProductItem(**p) for p in products],
                        )
                    )
            if len(rec_rows) < rows:
                trending = get_trending_rows(
                    session,
                    period="daily",
                    categories=rows - len(rec_rows),
                    products_per_category=products_per_row,
                )
                for category, products in trending:
                    rec_rows.append(
                        RecommendationRow(
                            row_label=category_row_label(category, trending=True),
                            products=[
                                ProductItem(**{k: p[k] for k in ProductItem.model_fields})
                                for p in products
                            ],
                        )
                    )
            response = HomepageResponse(
                user_id=user_id,
                rows=rec_rows[:rows],
                is_personalized=True,
                variant=variant,
            )

    product_ids = [item.product_id for row in response.rows for item in row.products]
    background_tasks.add_task(
        log_impression_task,
        request.app.state.engine,
        feature="homepage",
        user_id=user_id,
        product_ids=product_ids,
        metadata={"variant": variant, "personalized": response.is_personalized},
    )
    return response
