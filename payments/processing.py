import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.contracts import (
    MAX_ATTEMPTS,
    EventKind,
    PaymentEvent,
    PaymentStatus,
    WebhookPayload,
    WebhookStatus,
)
from payments.delivery.webhook import WebhookError, WebhookSender
from payments.persistence.db import Payment, Sessions, utcnow
from payments.persistence.repository import enqueue, payment_details

logger = logging.getLogger(__name__)


async def simulate_gateway() -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED


class UnknownPayment(Exception):
    pass


class PaymentProcessor:
    def __init__(
        self,
        sessions: Sessions,
        webhook: WebhookSender,
        retry_base_delay: float,
        gateway: Callable[[], Awaitable[PaymentStatus]] = simulate_gateway,
    ) -> None:
        self.sessions = sessions
        self.webhook = webhook
        self.retry_base_delay = retry_base_delay
        self.gateway = gateway

    def _retry_delay(self, attempt: int) -> timedelta:
        return timedelta(seconds=self.retry_base_delay * 2 ** (attempt - 1))

    async def process(self, event: PaymentEvent, *, internal_failure: bool = False) -> None:
        locked_payment = select(Payment).where(Payment.id == event.payment_id).with_for_update()

        async with self.sessions.begin() as session:
            payment = await session.scalar(locked_payment)
            if payment is None:
                raise UnknownPayment(event.payment_id)
            if payment.status == PaymentStatus.PENDING:
                if event.attempt != 1 or payment.gateway_failures + 1 != event.gateway_attempt:
                    return
                outcome = None if internal_failure else await self._run_gateway(payment.id)
                if outcome is None:
                    if self._gateway_failed(session, payment, event):
                        return
                    internal_failure = False
                else:
                    payment.status = outcome
                payment.processed_at = utcnow()

        async with self.sessions.begin() as session:
            payment = (await session.scalars(locked_payment)).one()
            if (
                payment.webhook_status != WebhookStatus.PENDING
                or payment.webhook_attempts + 1 != event.attempt
            ):
                return

            payment.webhook_attempts = event.attempt
            failure_code = "processing_error" if internal_failure else await self._deliver(payment)
            if failure_code is None:
                payment.webhook_status = WebhookStatus.DELIVERED
            else:
                self._webhook_failed(session, payment, event, failure_code)

    async def _run_gateway(self, payment_id: UUID) -> PaymentStatus | None:
        try:
            async with asyncio.timeout(10):
                outcome = await self.gateway()
            if outcome not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
                raise ValueError("Gateway did not return a final status")
        except Exception:
            logger.exception("Gateway error payment_id=%s", payment_id)
            return None
        return outcome

    def _gateway_failed(self, session: AsyncSession, payment: Payment, event: PaymentEvent) -> bool:
        payment.gateway_failures = event.gateway_attempt
        if event.gateway_attempt < MAX_ATTEMPTS:
            enqueue(
                session,
                payment.id,
                event.attempt,
                gateway_attempt=event.gateway_attempt + 1,
                available_at=utcnow() + self._retry_delay(event.gateway_attempt),
            )
            return True

        payment.status = PaymentStatus.FAILED
        enqueue(
            session,
            payment.id,
            event.gateway_attempt,
            gateway_attempt=event.gateway_attempt,
            kind=EventKind.DEAD_LETTER,
            error_code="gateway_processing_error",
        )
        return False

    async def _deliver(self, payment: Payment) -> str | None:
        try:
            await self.webhook.send(
                payment.webhook_url,
                WebhookPayload.from_payment(payment_details(payment)),
            )
        except WebhookError as exc:
            return str(exc)
        except Exception:
            logger.exception("Unexpected webhook error payment_id=%s", payment.id)
            return "webhook_internal_error"
        return None

    def _webhook_failed(
        self,
        session: AsyncSession,
        payment: Payment,
        event: PaymentEvent,
        code: str,
    ) -> None:
        logger.warning(
            "Webhook failed payment_id=%s attempt=%s code=%s",
            payment.id,
            event.attempt,
            code,
        )
        if event.attempt == MAX_ATTEMPTS:
            payment.webhook_status = WebhookStatus.EXHAUSTED
            enqueue(
                session,
                payment.id,
                event.attempt,
                kind=EventKind.DEAD_LETTER,
                error_code=code,
            )
        else:
            enqueue(
                session,
                payment.id,
                event.attempt + 1,
                available_at=utcnow() + self._retry_delay(event.attempt),
            )
