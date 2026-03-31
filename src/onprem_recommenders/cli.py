from __future__ import annotations

from pathlib import Path
from datetime import datetime

import typer
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import Settings, get_settings
from .db import create_db_engine, init_db
from .etl.autocomplete import materialize_query_suggestions
from .etl.derived_tables import materialize_derived_tables
from .etl.embeddings import sync_product_embeddings
from .etl.parquet_loader import load_source_tables
from .etl.search_index import sync_products_to_elasticsearch
from .infrastructure import compute_trending_products
from .models import Event, FeatureFlag

app = typer.Typer(help="On-Premise Recommenders CLI")
infrastructure_app = typer.Typer(help="Infrastructure management commands")
app.add_typer(infrastructure_app, name="infra")


def _resolve_settings(
    database_url: str | None = None,
    source_dir: Path | None = None,
    users_file: Path | None = None,
    products_file: Path | None = None,
    transactions_file: Path | None = None,
    interactions_file: Path | None = None,
) -> Settings:
    settings = get_settings()
    overrides = {}
    if database_url:
        overrides["database_url"] = database_url
    if source_dir:
        overrides["source_data_dir"] = source_dir
    if users_file:
        overrides["users_parquet_path"] = users_file
    if products_file:
        overrides["products_parquet_path"] = products_file
    if transactions_file:
        overrides["transactions_parquet_path"] = transactions_file
    if interactions_file:
        overrides["interactions_parquet_path"] = interactions_file
    resolved = settings.model_copy(update=overrides)
    resolved.ensure_local_dirs()
    return resolved


@app.command("init-db")
def init_db_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    typer.echo(f"Initialized database schema at {settings.database_url}")


@app.command("load-parquet")
def load_parquet_command(
    source_dir: Path | None = typer.Option(default=None),
    users_file: Path | None = typer.Option(default=None),
    products_file: Path | None = typer.Option(default=None),
    transactions_file: Path | None = typer.Option(default=None),
    interactions_file: Path | None = typer.Option(default=None),
    database_url: str | None = typer.Option(default=None),
) -> None:
    settings = _resolve_settings(
        database_url=database_url,
        source_dir=source_dir,
        users_file=users_file,
        products_file=products_file,
        transactions_file=transactions_file,
        interactions_file=interactions_file,
    )
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    source_paths = settings.parquet_paths()
    counts = load_source_tables(engine, source_paths)
    typer.echo("Loaded source tables from parquet files")
    for table_name, path in source_paths.items():
        typer.echo(f"  {table_name}: {path}")
    for table_name, count in counts.items():
        typer.echo(f"  {table_name}: {count}")


@app.command("build-derived")
def build_derived_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)
    typer.echo("Built derived tables")
    for table_name, count in counts.items():
        typer.echo(f"  {table_name}: {count}")


@app.command("sync-search")
def sync_search_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    indexed = sync_products_to_elasticsearch(
        engine,
        elasticsearch_url=settings.elasticsearch_url,
        index_name=settings.elasticsearch_index,
    )
    typer.echo(f"Indexed {indexed} products into Elasticsearch index '{settings.elasticsearch_index}'")


@app.command("sync-embeddings")
def sync_embeddings_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    synced = sync_product_embeddings(
        engine,
        openai_api_key=settings.openai_api_key,
        model=settings.openai_embedding_model,
        persist_directory=str(settings.chroma_persist_directory),
        collection_name=settings.chroma_collection,
        batch_size=settings.embedding_batch_size,
    )
    typer.echo(f"Synced {synced} product embeddings into Chroma collection '{settings.chroma_collection}'")


@app.command("build-autocomplete")
def build_autocomplete_command(database_url: str | None = typer.Option(default=None)) -> None:
    """Build query suggestions from search interactions for autocomplete.

    Creates global and category-specific query frequency tables used by
    the personalized autocomplete API.
    """
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    count = materialize_query_suggestions(engine)
    typer.echo(f"Built {count} query suggestions for autocomplete")


@app.command("run-stage1")
def run_stage1_command(
    source_dir: Path | None = typer.Option(default=None),
    users_file: Path | None = typer.Option(default=None),
    products_file: Path | None = typer.Option(default=None),
    transactions_file: Path | None = typer.Option(default=None),
    interactions_file: Path | None = typer.Option(default=None),
    database_url: str | None = typer.Option(default=None),
    skip_search: bool = typer.Option(default=False, help="Skip Elasticsearch indexing"),
    skip_embeddings: bool = typer.Option(default=False, help="Skip OpenAI + Chroma embedding sync"),
) -> None:
    settings = _resolve_settings(
        database_url=database_url,
        source_dir=source_dir,
        users_file=users_file,
        products_file=products_file,
        transactions_file=transactions_file,
        interactions_file=interactions_file,
    )
    engine = create_db_engine(settings.database_url)
    init_db(engine)

    source_paths = settings.parquet_paths()
    source_counts = load_source_tables(engine, source_paths)
    derived_counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)

    typer.echo("Stage 1 completed")
    typer.echo(f"  database: {settings.database_url}")
    for table_name, path in source_paths.items():
        typer.echo(f"  {table_name}_file: {path}")
    for table_name, count in {**source_counts, **derived_counts}.items():
        typer.echo(f"  {table_name}: {count}")

    if not skip_search:
        indexed = sync_products_to_elasticsearch(
            engine,
            elasticsearch_url=settings.elasticsearch_url,
            index_name=settings.elasticsearch_index,
        )
        typer.echo(f"  elasticsearch_documents: {indexed}")

    if not skip_embeddings:
        synced = sync_product_embeddings(
            engine,
            openai_api_key=settings.openai_api_key,
            model=settings.openai_embedding_model,
            persist_directory=str(settings.chroma_persist_directory),
            collection_name=settings.chroma_collection,
            batch_size=settings.embedding_batch_size,
        )
        typer.echo(f"  chroma_embeddings: {synced}")


@infrastructure_app.command("compute-trending")
def compute_trending_command(
    period: str = typer.Option(default="daily", help="Trending period (hourly, daily)"),
    top_n: int = typer.Option(default=10, help="Top N products per category"),
    categories: int = typer.Option(default=5, help="Number of top categories"),
    database_url: str | None = typer.Option(default=None),
) -> None:
    """Compute and store trending products for fallback recommendations."""
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    count = compute_trending_products(
        engine,
        period=period,
        top_n_per_category=top_n,
        top_categories=categories,
    )
    typer.echo(f"Computed {count} trending products for period '{period}'")


@infrastructure_app.command("list-events")
def list_events_command(
    feature: str | None = typer.Option(default=None, help="Filter by feature"),
    event_type: str | None = typer.Option(default=None, help="Filter by event type"),
    limit: int = typer.Option(default=20, help="Maximum number of events to show"),
    database_url: str | None = typer.Option(default=None),
) -> None:
    """List recent events from the event log."""
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)

    with Session(engine) as session:
        query = select(Event).order_by(Event.timestamp.desc())
        if feature:
            query = query.where(Event.feature == feature)
        if event_type:
            query = query.where(Event.event_type == event_type)
        query = query.limit(limit)
        events = session.execute(query).scalars().all()

    if not events:
        typer.echo("No events found")
        return

    typer.echo(f"Found {len(events)} events:")
    for e in events:
        typer.echo(
            f"  [{e.timestamp}] {e.feature}/{e.event_type} "
            f"user={e.user_id or 'anonymous'} "
            f"products={e.product_ids}"
        )


@infrastructure_app.command("list-flags")
def list_flags_command(
    database_url: str | None = typer.Option(default=None),
) -> None:
    """List all feature flags and their status."""
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)

    with Session(engine) as session:
        flags = session.execute(select(FeatureFlag)).scalars().all()

    if not flags:
        typer.echo("No feature flags found")
        return

    typer.echo(f"Found {len(flags)} feature flags:")
    for f in flags:
        status = "enabled" if f.enabled else "disabled"
        typer.echo(
            f"  {f.feature_name}: {f.variant} ({status}) "
            f"segment={f.user_segment or 'all'}"
        )


@infrastructure_app.command("set-flag")
def set_flag_command(
    feature_name: str,
    enabled: bool = typer.Option(default=True, help="Enable or disable the flag"),
    variant: str = typer.Option(default="control", help="A/B test variant"),
    description: str | None = typer.Option(default=None, help="Flag description"),
    database_url: str | None = typer.Option(default=None),
) -> None:
    """Create or update a feature flag."""
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)

    with Session(engine) as session:
        flag = session.execute(
            select(FeatureFlag).where(FeatureFlag.feature_name == feature_name)
        ).scalars().first()

        if flag:
            flag.enabled = enabled
            flag.variant = variant
            if description:
                flag.description = description
            flag.updated_at = datetime.utcnow()
            typer.echo(f"Updated feature flag '{feature_name}'")
        else:
            flag = FeatureFlag(
                feature_name=feature_name,
                variant=variant,
                enabled=enabled,
                description=description,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            session.add(flag)
            typer.echo(f"Created feature flag '{feature_name}'")

        session.commit()


@infrastructure_app.command("run-nightly")
def run_nightly_command(
    database_url: str | None = typer.Option(default=None),
) -> None:
    """Run all nightly maintenance tasks manually.

    This includes:
    - Derived tables recomputation
    - Trending products computation
    - Query suggestions update
    """
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    init_db(engine)

    typer.echo("Running nightly maintenance tasks...")

    # Derived tables
    typer.echo("  Building derived tables...")
    counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)
    for table_name, count in counts.items():
        typer.echo(f"    {table_name}: {count} rows")

    # Trending products
    typer.echo("  Computing trending products...")
    trending_count = compute_trending_products(
        engine,
        period="daily",
        top_n_per_category=10,
        top_categories=5,
    )
    typer.echo(f"    trending_products: {trending_count} products")

    # Query suggestions
    typer.echo("  Building query suggestions...")
    suggestions_count = materialize_query_suggestions(engine)
    typer.echo(f"    query_suggestions: {suggestions_count} suggestions")

    typer.echo("Nightly maintenance completed!")


def run() -> None:
    app()


if __name__ == "__main__":
    run()
