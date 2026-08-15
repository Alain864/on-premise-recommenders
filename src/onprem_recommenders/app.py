from __future__ import annotations

from contextlib import asynccontextmanager

from elasticsearch import Elasticsearch
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from onprem_recommenders.config import get_settings
from onprem_recommenders.db import create_db_engine
from onprem_recommenders.routers import autocomplete, events, flags, health, homepage, product, search
from onprem_recommenders.services.autocomplete import AutocompleteIndex


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    es = Elasticsearch(
        settings.elasticsearch_url,
        verify_certs=False,
        ssl_show_warn=False,
    )
    autocomplete_index = AutocompleteIndex()
    with Session(engine) as session:
        autocomplete_index.reload(session)

    app.state.settings = settings
    app.state.engine = engine
    app.state.es = es
    app.state.autocomplete = autocomplete_index
    try:
        yield
    finally:
        engine.dispose()
        es.close()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="On-Premise Recommenders",
        description="Search and recommendation system",
        version="0.2.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health.router)
    app.include_router(homepage.router)
    app.include_router(product.router)
    app.include_router(search.router)
    app.include_router(autocomplete.router)
    app.include_router(events.router)
    app.include_router(flags.router)
    return app


app = create_app()
