"""Persist independent gateway and outbox publication retry state."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payments", sa.Column("gateway_failures", sa.Integer(), server_default="0", nullable=False)
    )
    op.add_column(
        "outbox", sa.Column("gateway_attempt", sa.Integer(), server_default="1", nullable=False)
    )
    op.add_column(
        "outbox",
        sa.Column("publication_failures", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_check_constraint(
        "ck_payments_gateway_failures", "payments", "gateway_failures BETWEEN 0 AND 3"
    )
    op.create_check_constraint(
        "ck_outbox_gateway_attempt", "outbox", "gateway_attempt BETWEEN 1 AND 3"
    )
    op.create_check_constraint(
        "ck_outbox_publication_failures", "outbox", "publication_failures >= 0"
    )


def downgrade() -> None:
    op.drop_constraint("ck_outbox_publication_failures", "outbox")
    op.drop_constraint("ck_outbox_gateway_attempt", "outbox")
    op.drop_constraint("ck_payments_gateway_failures", "payments")
    op.drop_column("outbox", "publication_failures")
    op.drop_column("outbox", "gateway_attempt")
    op.drop_column("payments", "gateway_failures")
