from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    signup_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    country: Mapped[str] = mapped_column(String(8), nullable=False)


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    brand: Mapped[str] = mapped_column(String(256), nullable=False)
    price: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    category_path: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)


class Transaction(Base):
    __tablename__ = "transactions"

    order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class Interaction(Base):
    __tablename__ = "interactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    product_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    query_text: Mapped[str | None] = mapped_column(String(512), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class UserCategoryAffinity(Base):
    __tablename__ = "user_category_affinity"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    category_path: Mapped[str] = mapped_column(String(512), primary_key=True)
    purchase_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    view_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    add_to_cart_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    affinity_score: Mapped[float] = mapped_column(Float, nullable=False)
    last_signal_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class ProductStats(Base):
    __tablename__ = "product_stats"

    product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    view_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    add_to_cart_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    purchase_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    ctr_proxy: Mapped[float] = mapped_column(Float, nullable=False)
    conversion_rate: Mapped[float] = mapped_column(Float, nullable=False)
    review_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    review_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    in_stock: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    popularity_score: Mapped[float] = mapped_column(Float, nullable=False)
    last_signal_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class CoPurchasePair(Base):
    __tablename__ = "co_purchase_pairs"

    left_product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    right_product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pair_count: Mapped[int] = mapped_column(Integer, nullable=False)


class CoViewPair(Base):
    __tablename__ = "co_view_pairs"

    left_product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    right_product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pair_count: Mapped[int] = mapped_column(Integer, nullable=False)


class QuerySuggestion(Base):
    """Stores query text with frequency for autocomplete suggestions.

    Built from search interactions - aggregates query frequency globally
    and per-category for personalized suggestions.
    """

    __tablename__ = "query_suggestions"

    query_text: Mapped[str] = mapped_column(String(512), primary_key=True)
    frequency: Mapped[int] = mapped_column(Integer, nullable=False)
    category_path: Mapped[str | None] = mapped_column(String(512), nullable=True, primary_key=True)
    last_updated: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Event(Base):
    """Logs user interaction events for analytics and model retraining.

    Tracks impressions (products shown) and clicks (user actions) from
    all recommendation features.
    """

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    feature: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    product_ids: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array of product IDs
    query_text: Mapped[str | None] = mapped_column(String(512), nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)  # Additional metadata as JSON
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class FeatureFlag(Base):
    """Feature flags for A/B testing and gradual rollouts.

    Controls which variant of a feature is enabled for different
    user segments.
    """

    __tablename__ = "feature_flags"

    feature_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    variant: Mapped[str] = mapped_column(String(32), nullable=False, default="control")
    user_segment: Mapped[str | None] = mapped_column(String(64), nullable=True)  # e.g., "new_users", "power_users"
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TrendingProduct(Base):
    """Precomputed trending products for fallback recommendations.

    Updated periodically (hourly/daily) to provide fast fallback
    recommendations for anonymous and cold-start users.
    """

    __tablename__ = "trending_products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    category_path: Mapped[str | None] = mapped_column(String(512), nullable=True, index=True)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    period: Mapped[str] = mapped_column(String(32), nullable=False)  # e.g., "hourly", "daily"
    computed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

