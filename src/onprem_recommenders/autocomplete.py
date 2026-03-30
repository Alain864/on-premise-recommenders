"""Personalized Query Autocomplete API.

Stage 5: Feature 2 - Personalized Query Autocomplete

This module provides intelligent autocomplete suggestions based on:
1. Global query popularity (for anonymous users)
2. User's category affinities (for personalized suggestions)
3. Prefix matching for fast lookups
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from .config import get_settings
from .db import create_db_engine
from .models import QuerySuggestion, UserCategoryAffinity


router = APIRouter(prefix="/autocomplete", tags=["autocomplete"])


class AutocompleteSuggestion(BaseModel):
    """A single autocomplete suggestion."""

    query_text: str
    frequency: int
    relevance_score: float
    category_match: str | None = None


class AutocompleteResponse(BaseModel):
    """Response for autocomplete endpoint."""

    prefix: str
    suggestions: list[AutocompleteSuggestion]
    is_personalized: bool
    user_id: str | None = None


def _get_user_top_categories(session: Session, user_id: str, limit: int = 5) -> list[str]:
    """Get the user's top affinity categories."""
    affinities = (
        session.query(UserCategoryAffinity)
        .filter(UserCategoryAffinity.user_id == user_id)
        .order_by(UserCategoryAffinity.affinity_score.desc())
        .limit(limit)
        .all()
    )
    return [a.category_path for a in affinities]


def _build_prefix_condition(prefix: str) -> str:
    """Build SQL LIKE condition for prefix matching (case-insensitive)."""
    # Escape special LIKE characters
    escaped = prefix.replace("%", "\\%").replace("_", "\\_")
    return f"{escaped.lower()}%"


def get_suggestions(
    session: Session,
    prefix: str,
    user_id: str | None = None,
    limit: int = 10,
) -> tuple[list[AutocompleteSuggestion], bool]:
    """Get autocomplete suggestions for a prefix.

    For known users with category affinities, returns personalized suggestions
    boosted by their category preferences. For anonymous users or users without
    affinities, returns global popular queries.

    Returns:
        Tuple of (suggestions, is_personalized)
    """
    prefix_lower = prefix.lower().strip()
    like_pattern = f"{prefix_lower}%"

    if user_id:
        # Get user's top categories for personalization
        top_categories = _get_user_top_categories(session, user_id)

        if top_categories:
            # Personalized: boost queries from user's preferred categories
            # Use UNION to combine global and category-specific queries
            # with relevance scoring

            # Query for category-specific suggestions with boost
            category_suggestions = (
                session.query(
                    QuerySuggestion.query_text,
                    QuerySuggestion.frequency,
                    QuerySuggestion.category_path,
                )
                .filter(
                    and_(
                        func.lower(QuerySuggestion.query_text).like(like_pattern),
                        QuerySuggestion.category_path.in_(top_categories),
                    )
                )
                .all()
            )

            # Query for global suggestions (fallback)
            global_suggestions = (
                session.query(
                    QuerySuggestion.query_text,
                    QuerySuggestion.frequency,
                    QuerySuggestion.category_path,
                )
                .filter(
                    and_(
                        func.lower(QuerySuggestion.query_text).like(like_pattern),
                        QuerySuggestion.category_path.is_(None),
                    )
                )
                .all()
            )

            # Combine and deduplicate, preferring category-specific results
            suggestion_map: dict[str, AutocompleteSuggestion] = {}

            # Add global suggestions first (base score)
            for query_text, freq, _ in global_suggestions:
                suggestion_map[query_text] = AutocompleteSuggestion(
                    query_text=query_text,
                    frequency=freq,
                    relevance_score=0.0,
                    category_match=None,
                )

            # Add category-specific suggestions with boost
            for query_text, freq, category in category_suggestions:
                # Score based on category position (higher position = more boost)
                try:
                    cat_index = top_categories.index(category)
                except ValueError:
                    cat_index = len(top_categories)

                # Boost factor: 2.0 for top category, decreasing for others
                boost = 2.0 - (cat_index * 0.2)
                relevance_score = boost * (freq / 100.0)

                suggestion_map[query_text] = AutocompleteSuggestion(
                    query_text=query_text,
                    frequency=freq,
                    relevance_score=relevance_score,
                    category_match=category,
                )

            # Sort by relevance score (descending), then frequency (descending)
            suggestions = sorted(
                suggestion_map.values(),
                key=lambda s: (s.relevance_score, s.frequency),
                reverse=True,
            )[:limit]

            return suggestions, True

    # Anonymous user or no category affinity: return global popular queries
    global_suggestions = (
        session.query(
            QuerySuggestion.query_text,
            QuerySuggestion.frequency,
            QuerySuggestion.category_path,
        )
        .filter(
            and_(
                func.lower(QuerySuggestion.query_text).like(like_pattern),
                QuerySuggestion.category_path.is_(None),
            )
        )
        .order_by(QuerySuggestion.frequency.desc())
        .limit(limit)
        .all()
    )

    suggestions = [
        AutocompleteSuggestion(
            query_text=query_text,
            frequency=freq,
            relevance_score=0.0,
            category_match=None,
        )
        for query_text, freq, _ in global_suggestions
    ]

    return suggestions, False


def get_engine():
    """Get database engine from settings."""
    settings = get_settings()
    return create_db_engine(settings.database_url)


@router.get("/suggest", response_model=AutocompleteResponse)
def suggest(
    prefix: Annotated[str, Query(min_length=1, max_length=100, description="Search prefix to autocomplete")],
    user_id: Annotated[str | None, Query(description="User ID for personalization")] = None,
    limit: Annotated[int, Query(ge=1, le=20, description="Maximum number of suggestions")] = 10,
) -> AutocompleteResponse:
    """Get autocomplete suggestions for a search prefix.

    For authenticated users with category affinities, suggestions are personalized
    based on their browsing and purchase history. For anonymous users, returns
    globally popular queries matching the prefix.

    Algorithm:
    1. Match queries that start with the given prefix (case-insensitive)
    2. For known users: boost queries from their top affinity categories
    3. Score by relevance (personalization boost + frequency)
    4. Return top N suggestions

    Examples:
        - GET /autocomplete/suggest?prefix=sm&user_id=USR_123
        - GET /autocomplete/suggest?prefix=laptop&limit=5
    """
    engine = get_engine()
    
    with Session(engine) as session:
        suggestions, is_personalized = get_suggestions(
            session=session,
            prefix=prefix,
            user_id=user_id,
            limit=limit,
        )

    return AutocompleteResponse(
        prefix=prefix,
        suggestions=suggestions,
        is_personalized=is_personalized,
        user_id=user_id if is_personalized else None,
    )
