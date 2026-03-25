from __future__ import annotations

from pathlib import Path

import typer

from .config import Settings, get_settings
from .db import create_db_engine, init_db
from .etl.derived_tables import materialize_derived_tables
from .etl.embeddings import sync_product_embeddings
from .etl.parquet_loader import load_source_tables
from .etl.search_index import sync_products_to_elasticsearch

app = typer.Typer(help="Stage 1 data foundation pipeline")


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


def run() -> None:
    app()


if __name__ == "__main__":
    run()
