"""Celery tasks for Stage 6: nightly recomputation jobs.

This module provides:
- Nightly derived table recomputation
- Nightly trending products computation
- Nightly query suggestions update
- Nightly embedding sync
"""

from __future__ import annotations

import logging
from functools import lru_cache

from celery import Celery

from onprem_recommenders.config import get_settings
from onprem_recommenders.db import create_db_engine, init_db
from onprem_recommenders.etl.autocomplete import materialize_query_suggestions
from onprem_recommenders.etl.derived_tables import materialize_derived_tables
from onprem_recommenders.etl.embeddings import sync_product_embeddings
from onprem_recommenders.infrastructure import compute_trending_products

logger = logging.getLogger(__name__)


def get_celery_app() -> Celery:
    """Get Celery application instance."""
    settings = get_settings()
    broker_url = settings.celery_broker_url or settings.redis_url
    result_backend = settings.celery_result_backend or settings.redis_url

    app = Celery(
        "onprem_recommenders",
        broker=broker_url,
        backend=result_backend,
        include=["onprem_recommenders.tasks"],
    )

    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_time_limit=30 * 60,  # 30 minutes
        task_soft_time_limit=25 * 60,  # 25 minutes
    )

    # Configure beat schedule for nightly jobs
    app.conf.beat_schedule = {
        "nightly-derived-tables": {
            "task": "onprem_recommenders.tasks.recompute_derived_tables",
            "schedule": 60 * 60 * 24,  # Every 24 hours
            "args": (),
        },
        "nightly-trending-products": {
            "task": "onprem_recommenders.tasks.compute_trending",
            "schedule": 60 * 60,  # Every hour
            "args": (),
        },
        "nightly-query-suggestions": {
            "task": "onprem_recommenders.tasks.update_query_suggestions",
            "schedule": 60 * 60 * 24,  # Every 24 hours
            "args": (),
        },
    }

    return app


# Create Celery app instance
celery_app = get_celery_app()


@celery_app.task(bind=True, name="onprem_recommenders.tasks.recompute_derived_tables")
def recompute_derived_tables(self) -> dict:
    """Recompute derived tables (user_category_affinity, product_stats, etc.).

    This task should run nightly to update derived tables from new
    transaction and interaction data.
    """
    logger.info("Starting derived tables recomputation")
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    init_db(engine)

    try:
        counts = materialize_derived_tables(
            engine, session_gap_minutes=settings.session_gap_minutes
        )
        logger.info(f"Completed derived tables recomputation: {counts}")
        return {
            "status": "success",
            "tables_updated": list(counts.keys()),
            "row_counts": counts,
        }
    except Exception as e:
        logger.error(f"Failed to recompute derived tables: {e}")
        return {
            "status": "error",
            "error": str(e),
        }


@celery_app.task(bind=True, name="onprem_recommenders.tasks.compute_trending")
def compute_trending(self, period: str = "daily") -> dict:
    """Compute trending products for fallback recommendations.

    This task should run hourly to keep trending products fresh.
    """
    logger.info(f"Starting trending products computation for period '{period}'")
    settings = get_settings()
    engine = create_db_engine(settings.database_url)

    try:
        count = compute_trending_products(
            engine,
            period=period,
            top_n_per_category=10,
            top_categories=5,
        )
        logger.info(f"Completed trending computation: {count} products")
        return {
            "status": "success",
            "period": period,
            "products_count": count,
        }
    except Exception as e:
        logger.error(f"Failed to compute trending products: {e}")
        return {
            "status": "error",
            "error": str(e),
        }


@celery_app.task(bind=True, name="onprem_recommenders.tasks.update_query_suggestions")
def update_query_suggestions(self) -> dict:
    """Update query suggestions for autocomplete.

    This task should run nightly to refresh query frequency tables.
    """
    logger.info("Starting query suggestions update")
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    init_db(engine)

    try:
        count = materialize_query_suggestions(engine)
        logger.info(f"Completed query suggestions update: {count} suggestions")
        return {
            "status": "success",
            "suggestions_count": count,
        }
    except Exception as e:
        logger.error(f"Failed to update query suggestions: {e}")
        return {
            "status": "error",
            "error": str(e),
        }


@celery_app.task(bind=True, name="onprem_recommenders.tasks.sync_embeddings")
def sync_embeddings(self) -> dict:
    """Sync product embeddings to ChromaDB.

    This task can be run on-demand or nightly to update embeddings.
    """
    logger.info("Starting embeddings sync")
    settings = get_settings()
    engine = create_db_engine(settings.database_url)

    try:
        synced = sync_product_embeddings(
            engine,
            openai_api_key=settings.openai_api_key,
            model=settings.openai_embedding_model,
            persist_directory=str(settings.chroma_persist_directory),
            collection_name=settings.chroma_collection,
            batch_size=settings.embedding_batch_size,
        )
        logger.info(f"Completed embeddings sync: {synced} products")
        return {
            "status": "success",
            "products_synced": synced,
        }
    except Exception as e:
        logger.error(f"Failed to sync embeddings: {e}")
        return {
            "status": "error",
            "error": str(e),
        }


@celery_app.task(bind=True, name="onprem_recommenders.tasks.run_all_nightly")
def run_all_nightly(self) -> dict:
    """Run all nightly maintenance tasks in sequence.

    This is a convenience task that calls all other nightly tasks.
    """
    logger.info("Starting all nightly maintenance tasks")

    results = {}

    # Run tasks in sequence
    tasks = [
        ("derived_tables", recompute_derived_tables),
        ("trending", lambda: compute_trending("daily")),
        ("query_suggestions", update_query_suggestions),
    ]

    for name, task_func in tasks:
        try:
            result = task_func()
            results[name] = result
        except Exception as e:
            logger.error(f"Failed {name}: {e}")
            results[name] = {"status": "error", "error": str(e)}

    logger.info(f"Completed all nightly tasks: {results}")
    return {
        "status": "completed",
        "results": results,
    }