import hashlib
import json
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.contracts import (
    Currency,
    EventKind,
    PaymentAccepted,
    PaymentCreate,
    PaymentDetails,
    PaymentStatus,
    WebhookStatus,
)
from payments.persistence.db import Outbox, Payment, Sessions, utcnow


class IdempotencyConflict(Exception):
    pass


def enqueue(
    session: AsyncSession,
    payment_id: UUID,
    attempt: int = 1,
    *,
    available_at: datetime | None = None,
    gateway_attempt: int = 1,
    kind: EventKind = EventKind.PROCESS,
    error_code: str | None = None,
) -> None:
    session.add(
        Outbox(
            id=uuid4(),
            payment_id=payment_id,
            attempt=attempt,
            gateway_attempt=gateway_attempt,
            kind=kind,
            error_code=error_code,
            available_at=available_at or utcnow(),
        )
    )


def payment_details(payment: Payment) -> PaymentDetails:
    return PaymentDetails(
        payment_id=payment.id,
        status=PaymentStatus(payment.status),
        created_at=payment.created_at,
        amount=payment.amount,
        currency=Currency(payment.currency),
        description=payment.description,
        metadata=payment.extra_data,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
        webhook_status=WebhookStatus(payment.webhook_status),
        webhook_attempts=payment.webhook_attempts,
        gateway_failures=payment.gateway_failures,
    )


class PaymentRepository:
    def __init__(self, sessions: Sessions) -> None:
        self.sessions = sessions

    async def create(self, data: PaymentCreate, key: str) -> PaymentAccepted:
        payload = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        request_hash = hashlib.sha256(payload.encode()).hexdigest()

        async with self.sessions.begin() as session:
            statement = (
                insert(Payment)
                .values(
                    id=uuid4(),
                    idempotency_key=key,
                    request_hash=request_hash,
                    amount=data.amount,
                    currency=data.currency,
                    description=data.description,
                    extra_data=data.metadata,
                    webhook_url=str(data.webhook_url),
                )
                .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
                .returning(Payment.id, Payment.created_at)
            )
            created = (await session.execute(statement)).one_or_none()
            if created is not None:
                payment_id, created_at = created
                enqueue(session, payment_id)
                return PaymentAccepted(payment_id=payment_id, created_at=created_at)

            existing = (
                await session.scalars(select(Payment).where(Payment.idempotency_key == key))
            ).one()
            if existing.request_hash != request_hash:
                raise IdempotencyConflict
            return PaymentAccepted(payment_id=existing.id, created_at=existing.created_at)

    async def get(self, payment_id: UUID) -> PaymentDetails | None:
        async with self.sessions() as session:
            payment = await session.get(Payment, payment_id)
            return payment_details(payment) if payment is not None else None
