from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from onprem_recommenders.models import QuerySuggestion
from onprem_recommenders.schemas import AutocompleteSuggestion


@dataclass(frozen=True)
class SuggestionRow:
    query_text: str
    frequency: int
    category_path: str


class AutocompleteIndex:
    """In-process prefix index. Reloaded from Postgres, not queried with LIKE."""

    def __init__(self) -> None:
        self._rows: list[SuggestionRow] = []

    def __len__(self) -> int:
        return len(self._rows)

    def reload(self, session: Session) -> int:
        rows = session.execute(select(QuerySuggestion)).scalars().all()
        self._rows = [
            SuggestionRow(
                query_text=row.query_text,
                frequency=row.frequency,
                category_path=row.category_path or "",
            )
            for row in rows
        ]
        return len(self._rows)

    def suggest(
        self,
        prefix: str,
        *,
        user_categories: list[str] | None = None,
        limit: int = 10,
    ) -> tuple[list[AutocompleteSuggestion], bool]:
        needle = prefix.lower().strip()
        if not needle:
            return [], False

        personalized = bool(user_categories)
        suggestion_map: dict[str, AutocompleteSuggestion] = {}

        for row in self._rows:
            if not row.query_text.lower().startswith(needle):
                continue
            is_global = row.category_path == ""
            if is_global:
                existing = suggestion_map.get(row.query_text)
                if existing is None:
                    suggestion_map[row.query_text] = AutocompleteSuggestion(
                        query_text=row.query_text,
                        frequency=row.frequency,
                        relevance_score=0.0,
                        category_match=None,
                    )
                continue
            if not personalized or row.category_path not in user_categories:
                continue
            cat_index = user_categories.index(row.category_path)
            boost = 2.0 - (cat_index * 0.2)
            relevance = boost * (row.frequency / 100.0)
            current = suggestion_map.get(row.query_text)
            if current is None or relevance > current.relevance_score:
                suggestion_map[row.query_text] = AutocompleteSuggestion(
                    query_text=row.query_text,
                    frequency=row.frequency,
                    relevance_score=relevance,
                    category_match=row.category_path,
                )

        ranked = sorted(
            suggestion_map.values(),
            key=lambda item: (item.relevance_score, item.frequency),
            reverse=True,
        )[:limit]
        return ranked, personalized and any(item.relevance_score > 0 for item in ranked)
