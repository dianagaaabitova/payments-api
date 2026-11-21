from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import DBAPIError, InterfaceError
from sqlalchemy.exc import TimeoutError as PoolTimeout
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.contracts import EventKind

Sessions = async_sessionmaker[AsyncSession]


def utcnow() -> datetime:
    return datetime.now(UTC)


def create_engine(url: str) -> AsyncEngine:
    return create_async_engine(
        url,
        pool_pre_ping=True,
        hide_parameters=True,
        pool_timeout=5,
        connect_args={"timeout": 5, "command_timeout": 10},
    )


def is_transient_database_error(error: Exception) -> bool:
    if isinstance(error, OSError | PoolTimeout | InterfaceError):
        return True
    if isinstance(error, DBAPIError) and error.connection_invalidated:
        return True
    cause = error.orig if isinstance(error, DBAPIError) else error
    sqlstate = getattr(cause, "sqlstate", None)
    return isinstance(sqlstate, str) and (
        sqlstate.startswith(("08", "40", "53")) or sqlstate in {"55P03", "57P01", "57P02", "57P03"}
    )


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint(
            "(status = 'pending') = (processed_at IS NULL)",
            name="ck_payments_processed_at",
        ),
        CheckConstraint(
            "webhook_status IN ('pending', 'delivered', 'exhausted')",
            name="ck_payments_webhook_status",
        ),
        CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="ck_payments_webhook_attempts"),
        CheckConstraint("gateway_failures BETWEEN 0 AND 3", name="ck_payments_gateway_failures"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    extra_data: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    status: Mapped[str] = mapped_column(String(9), server_default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_status: Mapped[str] = mapped_column(String(9), server_default="pending")
    webhook_attempts: Mapped[int] = mapped_column(server_default="0")
    gateway_failures: Mapped[int] = mapped_column(server_default="0")


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("kind IN ('process', 'dead_letter')", name="ck_outbox_kind"),
        CheckConstraint("attempt BETWEEN 1 AND 3", name="ck_outbox_attempt"),
        CheckConstraint("gateway_attempt BETWEEN 1 AND 3", name="ck_outbox_gateway_attempt"),
        CheckConstraint("publication_failures >= 0", name="ck_outbox_publication_failures"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    kind: Mapped[EventKind] = mapped_column(String(11))
    attempt: Mapped[int]
    gateway_attempt: Mapped[int] = mapped_column(server_default="1")
    publication_failures: Mapped[int] = mapped_column(server_default="0")
    error_code: Mapped[str | None] = mapped_column(String(64))
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


Index(
    "ix_outbox_unpublished",
    Outbox.available_at,
    Outbox.id,
    postgresql_where=Outbox.published_at.is_(None),
)
