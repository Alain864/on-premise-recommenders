from typing import Sequence

revision: str = "001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    from alembic import op
    import sqlalchemy as sa
    from pgvector.sqlalchemy import Vector

    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "users",
        sa.Column("user_id", sa.String(64), primary_key=True),
        sa.Column("signup_date", sa.DateTime(), nullable=False),
        sa.Column("country", sa.String(8), nullable=False),
    )

    op.create_table(
        "products",
        sa.Column("product_id", sa.String(64), primary_key=True),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("brand", sa.String(256), nullable=False),
        sa.Column("price", sa.Numeric(12, 2), nullable=False),
        sa.Column("category_path", sa.String(512), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.Column("embedding_hash", sa.String(64), nullable=True),
    )
    op.create_index("ix_products_embedding_hash", "products", ["embedding_hash"])
    op.execute(
        "CREATE INDEX ix_products_embedding ON products "
        "USING hnsw (embedding vector_cosine_ops)"
    )

    op.create_table(
        "transactions",
        sa.Column("order_id", sa.String(64), primary_key=True),
        sa.Column("product_id", sa.String(64), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_transactions_user_id", "transactions", ["user_id"])
    op.create_index("ix_transactions_timestamp", "transactions", ["timestamp"])

    op.create_table(
        "interactions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("product_id", sa.String(64), nullable=True),
        sa.Column("query_text", sa.String(512), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_interactions_event_type", "interactions", ["event_type"])
    op.create_index("ix_interactions_user_id", "interactions", ["user_id"])
    op.create_index("ix_interactions_product_id", "interactions", ["product_id"])
    op.create_index("ix_interactions_timestamp", "interactions", ["timestamp"])

    op.create_table(
        "user_category_affinity",
        sa.Column("user_id", sa.String(64), primary_key=True),
        sa.Column("category_path", sa.String(512), primary_key=True),
        sa.Column("purchase_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("view_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("add_to_cart_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("affinity_score", sa.Float(), nullable=False),
        sa.Column("last_signal_at", sa.DateTime(), nullable=True),
    )

    op.create_table(
        "product_stats",
        sa.Column("product_id", sa.String(64), primary_key=True),
        sa.Column("view_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("add_to_cart_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("purchase_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("ctr_proxy", sa.Float(), nullable=False),
        sa.Column("conversion_rate", sa.Float(), nullable=False),
        sa.Column("review_score", sa.Float(), nullable=True),
        sa.Column("review_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("in_stock", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("popularity_score", sa.Float(), nullable=False),
        sa.Column("last_signal_at", sa.DateTime(), nullable=True),
    )

    op.create_table(
        "co_purchase_pairs",
        sa.Column("left_product_id", sa.String(64), primary_key=True),
        sa.Column("right_product_id", sa.String(64), primary_key=True),
        sa.Column("pair_count", sa.Integer(), nullable=False),
    )
    op.create_index("ix_co_purchase_right", "co_purchase_pairs", ["right_product_id"])

    op.create_table(
        "co_view_pairs",
        sa.Column("left_product_id", sa.String(64), primary_key=True),
        sa.Column("right_product_id", sa.String(64), primary_key=True),
        sa.Column("pair_count", sa.Integer(), nullable=False),
    )
    op.create_index("ix_co_view_right", "co_view_pairs", ["right_product_id"])

    op.create_table(
        "query_suggestions",
        sa.Column("query_text", sa.String(512), primary_key=True),
        sa.Column("category_path", sa.String(512), primary_key=True, server_default=""),
        sa.Column("frequency", sa.Integer(), nullable=False),
        sa.Column("last_updated", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_query_suggestions_text", "query_suggestions", ["query_text"])

    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.String(64), nullable=True),
        sa.Column("feature", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("product_ids", sa.Text(), nullable=True),
        sa.Column("query_text", sa.String(512), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_events_user_id", "events", ["user_id"])
    op.create_index("ix_events_feature", "events", ["feature"])
    op.create_index("ix_events_event_type", "events", ["event_type"])
    op.create_index("ix_events_timestamp", "events", ["timestamp"])

    op.create_table(
        "feature_flags",
        sa.Column("feature_name", sa.String(64), primary_key=True),
        sa.Column("variant", sa.String(32), nullable=False, server_default="control"),
        sa.Column("user_segment", sa.String(64), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("description", sa.String(256), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )

    op.create_table(
        "trending_products",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("product_id", sa.String(64), nullable=False),
        sa.Column("category_path", sa.String(512), nullable=True),
        sa.Column("root_category", sa.String(256), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("period", sa.String(32), nullable=False),
        sa.Column("computed_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_trending_products_product_id", "trending_products", ["product_id"])
    op.create_index("ix_trending_products_root_category", "trending_products", ["root_category"])
    op.create_index(
        "ix_trending_period_root_rank",
        "trending_products",
        ["period", "root_category", "rank"],
    )

    op.execute(
        """
        INSERT INTO feature_flags (feature_name, variant, enabled, description, created_at, updated_at)
        VALUES
          ('homepage', 'control', true, 'Personalized homepage feed', NOW(), NOW()),
          ('search_ranking', 'control', true, 'Weighted search re-rank and semantic fallback', NOW(), NOW()),
          ('product_page', 'control', true, 'Frequently bought together and also viewed', NOW(), NOW()),
          ('autocomplete', 'control', true, 'Personalized query autocomplete', NOW(), NOW())
        """
    )


def downgrade() -> None:
    from alembic import op

    op.drop_table("trending_products")
    op.drop_table("feature_flags")
    op.drop_table("events")
    op.drop_table("query_suggestions")
    op.drop_table("co_view_pairs")
    op.drop_table("co_purchase_pairs")
    op.drop_table("product_stats")
    op.drop_table("user_category_affinity")
    op.drop_table("interactions")
    op.drop_table("transactions")
    op.drop_table("products")
    op.drop_table("users")
    op.execute("DROP EXTENSION IF EXISTS vector")
