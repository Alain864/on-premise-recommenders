from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated

from elasticsearch import Elasticsearch
from fastapi import Depends, Request
from sqlalchemy.orm import Session

from onprem_recommenders.config import Settings
from onprem_recommenders.services.autocomplete import AutocompleteIndex


def get_settings_dep(request: Request) -> Settings:
    return request.app.state.settings


def get_engine_dep(request: Request):
    return request.app.state.engine


def get_session(request: Request) -> Iterator[Session]:
    session = Session(request.app.state.engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_es(request: Request) -> Elasticsearch:
    return request.app.state.es


def get_autocomplete_index(request: Request) -> AutocompleteIndex:
    return request.app.state.autocomplete


SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
SessionDep = Annotated[Session, Depends(get_session)]
EsDep = Annotated[Elasticsearch, Depends(get_es)]
AutocompleteDep = Annotated[AutocompleteIndex, Depends(get_autocomplete_index)]
