from __future__ import annotations

import logging

from celery import Celery

from onprem_recommenders.config import get_settings
from onprem_recommenders.db import create_db_engine
from onprem_recommenders.pipeline.derived import materialize_derived_tables
from onprem_recommenders.pipeline.embeddings import sync_product_embeddings
from onprem_recommenders.pipeline.search_index import sync_products_to_elasticsearch
from onprem_recommenders.pipeline.suggestions import materialize_query_suggestions
from onprem_recommenders.pipeline.trending import compute_trending_products

logger = logging.getLogger(__name__)


def get_celery_app() -> Celery:
    settings = get_settings()
    app = Celery(
        "onprem_recommenders",
        broker=settings.broker_url,
        backend=settings.result_backend,
        include=["onprem_recommenders.workers"],
    )
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_time_limit=30 * 60,
        task_soft_time_limit=25 * 60,
        beat_schedule={
            "nightly-derived-tables": {
                "task": "onprem_recommenders.workers.recompute_derived_tables",
                "schedule": 60 * 60 * 24,
            },
            "hourly-trending-products": {
                "task": "onprem_recommenders.workers.compute_trending",
                "schedule": 60 * 60,
            },
            "nightly-query-suggestions": {
                "task": "onprem_recommenders.workers.update_query_suggestions",
                "schedule": 60 * 60 * 24,
            },
            "nightly-embeddings": {
                "task": "onprem_recommenders.workers.sync_embeddings",
                "schedule": 60 * 60 * 24,
            },
        },
    )
    return app


celery_app = get_celery_app()


@celery_app.task(name="onprem_recommenders.workers.recompute_derived_tables")
def recompute_derived_tables() -> dict:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)
    logger.info("Derived tables recomputed: %s", counts)
    return {"status": "success", "row_counts": counts}


@celery_app.task(name="onprem_recommenders.workers.compute_trending")
def compute_trending(period: str = "daily") -> dict:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    count = compute_trending_products(engine, period=period)
    return {"status": "success", "products_count": count}


@celery_app.task(name="onprem_recommenders.workers.update_query_suggestions")
def update_query_suggestions() -> dict:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    count = materialize_query_suggestions(engine)
    return {"status": "success", "suggestions_count": count}


@celery_app.task(name="onprem_recommenders.workers.sync_embeddings")
def sync_embeddings() -> dict:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    synced = sync_product_embeddings(
        engine,
        openai_api_key=settings.openai_api_key,
        model=settings.openai_embedding_model,
        batch_size=settings.embedding_batch_size,
    )
    return {"status": "success", "products_synced": synced}


@celery_app.task(name="onprem_recommenders.workers.sync_search")
def sync_search() -> dict:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    indexed = sync_products_to_elasticsearch(
        engine, settings.elasticsearch_url, settings.elasticsearch_index
    )
    return {"status": "success", "indexed": indexed}


@celery_app.task(name="onprem_recommenders.workers.run_all_nightly")
def run_all_nightly() -> dict:
    """Enqueue nightly jobs. Call this task; do not invoke the others in-process."""
    return {
        "derived": recompute_derived_tables.delay().id,
        "trending": compute_trending.delay().id,
        "suggestions": update_query_suggestions.delay().id,
        "embeddings": sync_embeddings.delay().id,
    }
