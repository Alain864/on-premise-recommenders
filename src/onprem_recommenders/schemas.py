from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ProductItem(BaseModel):
    product_id: str
    title: str
    brand: str
    price: float
    category_path: str
    popularity_score: float


class ProductDetail(BaseModel):
    product_id: str
    title: str
    brand: str
    price: float
    category_path: str
    description: str | None = None
    popularity_score: float = 0.0


class RecommendationRow(BaseModel):
    row_label: str
    products: list[ProductItem]


class HomepageResponse(BaseModel):
    user_id: str
    rows: list[RecommendationRow]
    is_personalized: bool
    variant: str | None = None


class ProductPageResponse(BaseModel):
    product_id: str
    recommendations: list[ProductItem]
    recommendation_type: str
    fallback: bool
    variant: str | None = None


class SearchProductItem(ProductItem):
    bm25_score: float | None = None
    final_score: float | None = None


class SearchResponse(BaseModel):
    query: str
    user_id: str | None
    results: list[SearchProductItem]
    total_hits: int
    is_personalized: bool
    used_semantic_fallback: bool
    variant: str | None = None


class AutocompleteSuggestion(BaseModel):
    query_text: str
    frequency: int
    relevance_score: float
    category_match: str | None = None


class AutocompleteResponse(BaseModel):
    prefix: str
    suggestions: list[AutocompleteSuggestion]
    is_personalized: bool
    user_id: str | None = None
    variant: str | None = None


class EventRequest(BaseModel):
    user_id: str | None = None
    feature: str
    event_type: str
    product_ids: list[str] | None = None
    query_text: str | None = None
    metadata: dict[str, Any] | None = None


class EventResponse(BaseModel):
    success: bool
    event_id: int | None = None
    message: str


class FeatureFlagResponse(BaseModel):
    feature_name: str
    variant: str
    user_segment: str | None
    enabled: bool
    description: str | None


class FeatureFlagUpdate(BaseModel):
    enabled: bool | None = None
    variant: str | None = Field(default=None, max_length=32)
    description: str | None = None


class TrendingRow(BaseModel):
    category: str
    products: list[ProductItem]


class TrendingResponse(BaseModel):
    period: str
    rows: list[TrendingRow]
