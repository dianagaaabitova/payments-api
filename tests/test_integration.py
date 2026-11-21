import asyncio
import hmac
import json
import os
from collections import defaultdict
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request, Response
from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from payments.contracts import EventKind, WebhookPayload
from payments.delivery.messaging import dead_queue, exchange, queue
from payments.persistence.db import Outbox, create_engine
from payments.settings import get_settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="Run through Docker Compose"),
]


@pytest.fixture
async def receiver() -> AsyncIterator[dict[str, list[tuple[bytes, str, str]]]]:
    calls: dict[str, list[tuple[bytes, str, str]]] = defaultdict(list)
    app = FastAPI()

    @app.post("/{scenario}")
    async def receive(scenario: str, request: Request) -> Response:
        calls[scenario].append(
            (
                await request.body(),
                request.headers["X-Webhook-Signature"],
                request.headers["X-Webhook-Id"],
            ),
        )
        failed = scenario == "exhausted" or len(calls[scenario]) == 1
        return Response(status_code=503 if failed else 204)

    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8081, log_level="error"))
    task = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(5):
            while not server.started:  # noqa: ASYNC110
                await asyncio.sleep(0.05)
        yield calls
    finally:
        server.should_exit = True
        await task


async def wait_for_webhook(
    client: httpx.AsyncClient,
    location: str,
    status: str,
) -> dict[str, Any]:
    async with asyncio.timeout(45):
        while True:
            response = await client.get(location)
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            if result["webhook_status"] == status:
                return result
            await asyncio.sleep(0.2)


@pytest.mark.parametrize(("scenario", "attempts"), [("delivered", 2), ("exhausted", 3)])
async def test_payment_lifecycle(
    receiver: dict[str, list[tuple[bytes, str, str]]],
    scenario: str,
    attempts: int,
) -> None:
    settings = get_settings()
    amount = "1234567890123456.78"
    payload = {
        "amount": amount,
        "currency": "EUR",
        "description": "order",
        "metadata": {"source": "integration"},
        "webhook_url": f"http://test:8081/{scenario}",
    }
    path = "/api/v1/payments"
    key = {"Idempotency-Key": str(uuid4())}
    engine = create_engine(settings.database_url)
    sessions = async_sessionmaker(engine)
    async with httpx.AsyncClient(base_url="http://api:8000", trust_env=False) as client:
        assert (await client.post(path, json=payload, headers=key)).status_code == 401
        client.headers["X-API-Key"] = settings.api_key.get_secret_value()
        try:
            assert (await client.post(path, json=payload)).status_code == 422
            assert (await client.get(f"{path}/{uuid4()}")).status_code == 404
            too_large = await client.post(
                path,
                content=b" " * 65537,
                headers=key,
            )
            assert too_large.status_code == 413
            numeric_payload = json.dumps(payload).replace(json.dumps(amount), amount, 1)
            first, second = await asyncio.gather(
                client.post(path, json=payload, headers=key),
                client.post(
                    path,
                    content=numeric_payload,
                    headers={**key, "Content-Type": "application/json"},
                ),
            )
            assert first.status_code == second.status_code == 202
            assert first.json() == second.json()
            assert first.json()["status"] == "pending"
            payment_id = UUID(first.json()["payment_id"])
            location = first.headers["Location"]
            changed = await client.post(path, json={**payload, "amount": "146"}, headers=key)
            assert changed.status_code == 409

            result = await wait_for_webhook(client, location, scenario)
            assert result["status"] in {"succeeded", "failed"}
            assert result["amount"] == payload["amount"]
            assert result["webhook_attempts"] == attempts
            deliveries = receiver[scenario]
            assert len(deliveries) == attempts
            assert len(set(deliveries)) == 1
            body, signature, event_id = deliveries[0]
            secret = settings.webhook_signing_secret.get_secret_value().encode()
            assert signature == f"sha256={hmac.digest(secret, body, 'sha256').hex()}"
            assert str(payment_id) == event_id
            webhook = WebhookPayload.model_validate_json(body)
            assert webhook.status == result["status"]

            async with sessions() as session:
                events = {
                    (event.kind, event.attempt)
                    for event in await session.scalars(
                        select(Outbox).where(Outbox.payment_id == payment_id)
                    )
                }
            expected_events = {(EventKind.PROCESS, n) for n in range(1, attempts + 1)}
            async with RabbitBroker(settings.rabbitmq_url) as broker:
                if scenario == "exhausted":
                    expected_events.add((EventKind.DEAD_LETTER, 3))
                    dlq = await broker.declare_queue(dead_queue)
                    async with dlq.iterator() as iterator, asyncio.timeout(12):
                        async for message in iterator:
                            if message.correlation_id == str(payment_id):
                                await message.ack()
                                assert b'"error_code":"http_503"' in message.body.replace(
                                    b" ",
                                    b"",
                                )
                                break
                            await message.ack()
                else:
                    # A duplicate broker delivery after completion must be a no-op.
                    await broker.publish(
                        {"event_id": str(uuid4()), "payment_id": str(payment_id), "attempt": 1},
                        exchange=exchange,
                        routing_key=queue.name,
                        mandatory=True,
                    )
                    await asyncio.sleep(0.75)
                    assert len(receiver[scenario]) == attempts
                    assert (await client.get(location)).json() == result
            assert events == expected_events
        finally:
            await engine.dispose()
