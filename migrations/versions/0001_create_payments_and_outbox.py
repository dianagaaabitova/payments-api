"""Create payments and outbox tables."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("idempotency_key", sa.String(128), nullable=False, unique=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", JSONB(), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column("status", sa.String(9), nullable=False, server_default="pending"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_status", sa.String(9), nullable=False, server_default="pending"),
        sa.Column("webhook_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)",
            name="ck_payments_processed_at",
        ),
        sa.CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'exhausted')",
            name="ck_payments_webhook_status",
        ),
        sa.CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="ck_payments_webhook_attempts"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("kind", sa.String(11), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(64)),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("kind IN ('process', 'dead_letter')", name="ck_outbox_kind"),
        sa.CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["available_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("payments")
