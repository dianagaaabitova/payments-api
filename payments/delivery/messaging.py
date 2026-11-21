import asyncio
import logging
from datetime import timedelta

from faststream.rabbit import (
    Channel,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
)
from pamqp.commands import Basic
from pydantic import ValidationError
from sqlalchemy import func, select

from payments.contracts import EventKind, PaymentEvent
from payments.persistence.db import Outbox, Sessions, is_transient_database_error, utcnow
from payments.processing import PaymentProcessor, UnknownPayment

logger = logging.getLogger(__name__)

exchange = RabbitExchange("payments", durable=True)
dead_exchange = RabbitExchange("payments.dead", durable=True)
queue = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    arguments={
        "x-dead-letter-exchange": dead_exchange.name,
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
dead_queue = RabbitQueue("payments.dlq", queue_type=QueueType.QUORUM, durable=True)


def create_broker(url: str, processor: PaymentProcessor) -> RabbitBroker:
    broker = RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=40,
    )

    async def raw_body(message: RabbitMessage) -> bytes:
        return message.raw_message.body

    @broker.subscriber(queue, exchange, no_ack=True, decoder=raw_body)
    async def consume(message: RabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate_json(message.raw_message.body)
        except ValidationError:
            logger.warning("Rejecting invalid payment event")
            await message.reject(requeue=False)
            return

        try:
            await processor.process(event)
        except UnknownPayment:
            logger.warning("Payment not found for broker event payment_id=%s", event.payment_id)
            await message.reject(requeue=False)
            return
        except Exception as exc:
            if is_transient_database_error(exc):
                await requeue_after_database_failure(message, event)
                return
            logger.exception("Processing error payment_id=%s", event.payment_id)
            try:
                await processor.process(event, internal_failure=True)
            except Exception as state_error:
                if is_transient_database_error(state_error):
                    await requeue_after_database_failure(message, event)
                else:
                    logger.exception(
                        "Could not record processing failure payment_id=%s", event.payment_id
                    )
                    await message.reject(requeue=False)
                return
        await message.ack()

    return broker


async def requeue_after_database_failure(message: RabbitMessage, event: PaymentEvent) -> None:
    logger.exception("Transient database failure payment_id=%s", event.payment_id)
    await asyncio.sleep(2)
    await message.nack(requeue=True)


async def declare_dead_letter_topology(broker: RabbitBroker) -> None:
    target = await broker.declare_exchange(dead_exchange)
    dlq = await broker.declare_queue(dead_queue)
    await dlq.bind(target, routing_key=dead_queue.name)


async def publish_next(sessions: Sessions, broker: RabbitBroker) -> bool:
    async with sessions.begin() as session:
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False

        payload = PaymentEvent(
            event_id=event.id,
            payment_id=event.payment_id,
            attempt=event.attempt,
            gateway_attempt=event.gateway_attempt,
        ).model_dump(mode="json", exclude_defaults=True)
        if event.kind == EventKind.DEAD_LETTER:
            payload["error_code"] = event.error_code
            target, routing_key = dead_exchange, dead_queue.name
        else:
            target, routing_key = exchange, queue.name

        try:
            async with asyncio.timeout(5):
                confirmation = await broker.publish(
                    payload,
                    exchange=target,
                    routing_key=routing_key,
                    message_id=str(event.id),
                    correlation_id=str(event.payment_id),
                    persist=True,
                    mandatory=True,
                )
            if not isinstance(confirmation, Basic.Ack):
                raise RuntimeError("RabbitMQ did not confirm the Outbox event")
        except Exception:
            event.publication_failures += 1
            delay = min(60, 2 ** min(event.publication_failures, 6))
            event.available_at = utcnow() + timedelta(seconds=delay)
            logger.exception(
                "Outbox publish failed event_id=%s route=%s retry_in=%ss",
                event.id,
                routing_key,
                delay,
            )
            return True

        event.published_at = utcnow()
        return True


async def relay(sessions: Sessions, broker: RabbitBroker) -> None:
    while True:
        try:
            if not await publish_next(sessions, broker):
                await asyncio.sleep(0.25)
        except Exception:
            logger.exception("Outbox database operation failed")
            await asyncio.sleep(2)
