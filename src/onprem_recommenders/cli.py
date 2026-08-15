from __future__ import annotations

from pathlib import Path

import typer
from alembic import command
from alembic.config import Config
from sqlalchemy.orm import Session

from onprem_recommenders.config import Settings, get_settings
from onprem_recommenders.db import create_db_engine
from onprem_recommenders.models import Event, FeatureFlag
from onprem_recommenders.pipeline.derived import materialize_derived_tables
from onprem_recommenders.pipeline.embeddings import sync_product_embeddings
from onprem_recommenders.pipeline.load import load_source_tables
from onprem_recommenders.pipeline.search_index import sync_products_to_elasticsearch
from onprem_recommenders.pipeline.suggestions import materialize_query_suggestions
from onprem_recommenders.pipeline.trending import compute_trending_products
from onprem_recommenders.services.flags import invalidate

app = typer.Typer(help="On-premise recommenders CLI")
infra_app = typer.Typer(help="Infrastructure commands")
app.add_typer(infra_app, name="infra")


def _alembic_config(database_url: str) -> Config:
    root = Path(__file__).resolve().parents[2]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def _settings(
    database_url: str | None = None,
    source_dir: Path | None = None,
) -> Settings:
    settings = get_settings()
    updates = {}
    if database_url:
        updates["database_url"] = database_url
    if source_dir:
        updates["source_data_dir"] = source_dir
    return settings.model_copy(update=updates) if updates else settings


@app.command("init-db")
def init_db_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    command.upgrade(_alembic_config(settings.database_url), "head")
    typer.echo(f"Migrated database at {settings.database_url}")


@app.command("load-parquet")
def load_parquet_command(
    source_dir: Path | None = typer.Option(default=None),
    database_url: str | None = typer.Option(default=None),
) -> None:
    settings = _settings(database_url=database_url, source_dir=source_dir)
    engine = create_db_engine(settings.database_url)
    counts = load_source_tables(engine, settings.parquet_paths())
    for name, count in counts.items():
        typer.echo(f"  {name}: {count}")


@app.command("build-derived")
def build_derived_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)
    for name, count in counts.items():
        typer.echo(f"  {name}: {count}")


@app.command("sync-search")
def sync_search_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    indexed = sync_products_to_elasticsearch(
        engine, settings.elasticsearch_url, settings.elasticsearch_index
    )
    typer.echo(f"Indexed {indexed} products")


@app.command("sync-embeddings")
def sync_embeddings_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    synced = sync_product_embeddings(
        engine,
        openai_api_key=settings.openai_api_key,
        model=settings.openai_embedding_model,
        batch_size=settings.embedding_batch_size,
    )
    typer.echo(f"Synced {synced} embeddings")


@app.command("build-autocomplete")
def build_autocomplete_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    count = materialize_query_suggestions(engine)
    typer.echo(f"Built {count} query suggestions")


@app.command("run-pipeline")
def run_pipeline_command(
    source_dir: Path | None = typer.Option(default=None),
    database_url: str | None = typer.Option(default=None),
    skip_search: bool = typer.Option(default=False),
    skip_embeddings: bool = typer.Option(default=False),
) -> None:
    settings = _settings(database_url=database_url, source_dir=source_dir)
    command.upgrade(_alembic_config(settings.database_url), "head")
    engine = create_db_engine(settings.database_url)

    source_counts = load_source_tables(engine, settings.parquet_paths())
    derived_counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)
    suggestion_count = materialize_query_suggestions(engine)
    trending_count = compute_trending_products(engine)

    typer.echo("Pipeline completed")
    for name, count in {**source_counts, **derived_counts}.items():
        typer.echo(f"  {name}: {count}")
    typer.echo(f"  query_suggestions: {suggestion_count}")
    typer.echo(f"  trending_products: {trending_count}")

    if not skip_search:
        indexed = sync_products_to_elasticsearch(
            engine, settings.elasticsearch_url, settings.elasticsearch_index
        )
        typer.echo(f"  elasticsearch_documents: {indexed}")
    if not skip_embeddings:
        synced = sync_product_embeddings(
            engine,
            openai_api_key=settings.openai_api_key,
            model=settings.openai_embedding_model,
            batch_size=settings.embedding_batch_size,
        )
        typer.echo(f"  embeddings: {synced}")


@infra_app.command("compute-trending")
def compute_trending_command(
    period: str = typer.Option(default="daily"),
    top_n: int = typer.Option(default=10),
    categories: int = typer.Option(default=5),
    database_url: str | None = typer.Option(default=None),
) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    count = compute_trending_products(
        engine, period=period, top_n_per_category=top_n, top_categories=categories
    )
    typer.echo(f"Computed {count} trending products")


@infra_app.command("list-events")
def list_events_command(
    feature: str | None = typer.Option(default=None),
    event_type: str | None = typer.Option(default=None),
    limit: int = typer.Option(default=20),
    database_url: str | None = typer.Option(default=None),
) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    with Session(engine) as session:
        query = session.query(Event).order_by(Event.timestamp.desc())
        if feature:
            query = query.filter(Event.feature == feature)
        if event_type:
            query = query.filter(Event.event_type == event_type)
        events = query.limit(limit).all()
    if not events:
        typer.echo("No events found")
        return
    for event in events:
        typer.echo(
            f"  [{event.timestamp}] {event.feature}/{event.event_type} "
            f"user={event.user_id or 'anonymous'}"
        )


@infra_app.command("list-flags")
def list_flags_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    with Session(engine) as session:
        flags = session.query(FeatureFlag).all()
    for flag in flags:
        status = "enabled" if flag.enabled else "disabled"
        typer.echo(f"  {flag.feature_name}: {flag.variant} ({status})")


@infra_app.command("set-flag")
def set_flag_command(
    feature_name: str,
    enabled: bool = typer.Option(default=True),
    variant: str = typer.Option(default="control"),
    database_url: str | None = typer.Option(default=None),
) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    with Session(engine) as session:
        flag = session.get(FeatureFlag, feature_name)
        if flag is None:
            typer.echo(f"Unknown feature '{feature_name}'")
            raise typer.Exit(code=1)
        flag.enabled = enabled
        flag.variant = variant
        session.commit()
    invalidate(feature_name)
    typer.echo(f"Updated {feature_name}")


@infra_app.command("run-nightly")
def run_nightly_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)
    for name, count in counts.items():
        typer.echo(f"  {name}: {count}")
    trending_count = compute_trending_products(engine)
    typer.echo(f"  trending_products: {trending_count}")
    suggestions = materialize_query_suggestions(engine)
    typer.echo(f"  query_suggestions: {suggestions}")


def run() -> None:
    app()


if __name__ == "__main__":
    run()
