from __future__ import annotations

from pathlib import Path

import typer

from .config import Settings, get_settings
from .db import create_db_engine, init_db
from .etl.derived_tables import materialize_derived_tables
from .etl.embeddings import sync_product_embeddings
from .etl.excel_loader import load_source_tables
from .etl.search_index import sync_products_to_elasticsearch

app = typer.Typer(help="Stage 1 data foundation pipeline")


def _resolve_settings(
    database_url: str | None = None,
    workbook: Path | None = None,
) -> Settings:
    settings = get_settings()
    overrides = {}
    if database_url:
        overrides["database_url"] = database_url
    if workbook:
        overrides["source_workbook"] = workbook
    resolved = settings.model_copy(update=overrides)
    resolved.ensure_local_dirs()
    return resolved


@app.command("init-db")
def init_db_command(database_url: str | None = typer.Option(default=None)) -> None:
    settings = _resolve_settings(database_url=database_url)
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    typer.echo(f"Initialized database schema at {settings.database_url}")


@app.command("load-source")
def load_source_command(
    workbook: Path | None = typer.Option(default=None),
    database_url: str | None = typer.Option(default=None),
) -> None:
    settings = _resolve_settings(database_url=database_url, workbook=workbook)
    engine = create_db_engine(settings.database_url)
    init_db(engine)
    counts = load_source_tables(engine, settings.source_workbook)
    typer.echo(f"Loaded source tables from {settings.source_workbook}")
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


@app.command("run-stage1")
def run_stage1_command(
    workbook: Path | None = typer.Option(default=None),
    database_url: str | None = typer.Option(default=None),
    skip_search: bool = typer.Option(default=False, help="Skip Elasticsearch indexing"),
    skip_embeddings: bool = typer.Option(default=False, help="Skip OpenAI + Chroma embedding sync"),
) -> None:
    settings = _resolve_settings(database_url=database_url, workbook=workbook)
    engine = create_db_engine(settings.database_url)
    init_db(engine)

    source_counts = load_source_tables(engine, settings.source_workbook)
    derived_counts = materialize_derived_tables(engine, session_gap_minutes=settings.session_gap_minutes)

    typer.echo("Stage 1 completed")
    typer.echo(f"  database: {settings.database_url}")
    typer.echo(f"  workbook: {settings.source_workbook}")
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


def run() -> None:
    app()


if __name__ == "__main__":
    run()

