from typing import Sequence

revision: str = "002_widen_product_title"
down_revision: str | None = "001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    from alembic import op
    import sqlalchemy as sa

    op.alter_column(
        "products",
        "title",
        existing_type=sa.String(512),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade() -> None:
    from alembic import op
    import sqlalchemy as sa

    op.alter_column(
        "products",
        "title",
        existing_type=sa.Text(),
        type_=sa.String(512),
        existing_nullable=False,
    )
