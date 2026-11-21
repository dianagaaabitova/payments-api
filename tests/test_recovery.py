import os
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from payments.contracts import EventKind, PaymentCreate, PaymentEvent, PaymentStatus, WebhookStatus
from payments.delivery.messaging import publish_next
from payments.persistence.db import Base, Outbox, Sessions, create_engine, utcnow
from payments.persistence.repository import PaymentRepository, enqueue
from payments.processing import PaymentProcessor
from payments.settings import get_settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="Run through Docker Compose"),
]


@pytest.fixture
async def sessions() -> AsyncIterator[Sessions]:
    engine = create_engine(get_settings().database_url)
    schema = f"test_{uuid4().hex}"
    isolated = engine.execution_options(schema_translate_map={None: schema})
    try:
        async with isolated.begin() as conn:
            await conn.execute(CreateSchema(schema))
            await conn.run_sync(Base.metadata.create_all)
        yield async_sessionmaker(isolated, expire_on_commit=False)
    finally:
        try:
            async with engine.begin() as conn:
                await conn.execute(DropSchema(schema, cascade=True, if_exists=True))
        finally:
            await engine.dispose()


async def test_unroutable_event_does_not_starve_later_outbox_events(sessions: Sessions) -> None:
    repo = PaymentRepository(sessions)
    data = PaymentCreate.model_validate(
        {
            "amount": "12",
            "currency": "EUR",
            "description": "First payment",
            "webhook_url": "https://example.com/callback",
        },
    )
    first = await repo.create(data, "first")
    async with sessions.begin() as session:
        pending = (await session.scalars(select(Outbox))).one()
        pending.published_at = utcnow()
        enqueue(
            session,
            first.payment_id,
            3,
            kind=EventKind.DEAD_LETTER,
            error_code="timeout",
        )
    second = await repo.create(data, "second")

    broker = AsyncMock(spec=RabbitBroker)
    broker.publish.side_effect = [RuntimeError("unroutable"), Basic.Ack(delivery_tag=1)]
    assert await publish_next(sessions, broker)
    assert await publish_next(sessions, broker)
    async with sessions() as session:
        parked = (
            await session.scalars(select(Outbox).where(Outbox.kind == EventKind.DEAD_LETTER))
        ).one()
        assert parked.published_at is None
        assert parked.publication_failures == 1
        assert parked.available_at > utcnow()
        later = (
            await session.scalars(select(Outbox).where(Outbox.payment_id == second.payment_id))
        ).one()
        assert later.published_at is not None


@pytest.mark.parametrize("failing_stage", ["gateway", "webhook"])
async def test_processing_exception_finishes_with_consistent_state(
    sessions: Sessions,
    failing_stage: str,
) -> None:
    repo = PaymentRepository(sessions)
    payment = await repo.create(
        PaymentCreate.model_validate(
            {
                "amount": "12",
                "currency": "EUR",
                "description": "Recovery test",
                "webhook_url": "https://example.com/callback",
            },
        ),
        "recover",
    )
    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    webhook = AsyncMock()
    if failing_stage == "gateway":
        gateway.side_effect = RuntimeError("gateway failure")
    else:
        webhook.send.side_effect = RuntimeError("webhook failure")

    for number in range(1, 4):
        event = PaymentEvent(
            event_id=uuid4(),
            payment_id=payment.payment_id,
            attempt=number if failing_stage == "webhook" else 1,
            gateway_attempt=number if failing_stage == "gateway" else 1,
        )
        processor = PaymentProcessor(sessions, webhook, retry_base_delay=2, gateway=gateway)
        await processor.process(event)
        await processor.process(event)

    result = await repo.get(payment.payment_id)
    assert result is not None
    assert result.processed_at is not None
    if failing_stage == "gateway":
        assert result.status == PaymentStatus.FAILED
        assert result.webhook_status == WebhookStatus.DELIVERED
        assert result.gateway_failures == gateway.await_count == 3
        assert result.webhook_attempts == webhook.send.await_count == 1
    else:
        assert result.status == PaymentStatus.SUCCEEDED
        assert result.webhook_status == WebhookStatus.EXHAUSTED
        assert result.gateway_failures == 0
        assert gateway.await_count == 1
        assert result.webhook_attempts == webhook.send.await_count == 3
    async with sessions() as session:
        dead = (
            await session.scalars(select(Outbox).where(Outbox.kind == EventKind.DEAD_LETTER))
        ).one()
        assert dead.attempt == 3
        assert dead.error_code == (
            "gateway_processing_error" if failing_stage == "gateway" else "webhook_internal_error"
        )
