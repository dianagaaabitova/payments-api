from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from payments.contracts import Currency, PaymentStatus, WebhookPayload
from payments.delivery.webhook import HttpWebhookSender


async def test_webhook_connect_failure_uses_another_checked_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender = HttpWebhookSender("test-secret", 5, frozenset())
    monkeypatch.setattr(
        sender,
        "_resolve",
        AsyncMock(return_value=["192.0.2.1", "192.0.2.2"]),
    )
    visited: list[str] = []

    def receive(request: httpx.Request) -> httpx.Response:
        assert request.headers["Host"] == "receiver.example"
        assert request.extensions["sni_hostname"] == "receiver.example"
        visited.append(request.url.host)
        if request.url.host == "192.0.2.1":
            raise httpx.ConnectError("first address unavailable")
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(receive), trust_env=False)
    monkeypatch.setattr("payments.delivery.webhook.httpx.AsyncClient", lambda **_: client)
    payment_id = uuid4()
    await sender.send(
        "https://receiver.example/callback",
        WebhookPayload(
            event_id=payment_id,
            payment_id=payment_id,
            status=PaymentStatus.SUCCEEDED,
            amount=Decimal("10.00"),
            currency=Currency.RUB,
            processed_at=datetime.now(UTC),
        ),
    )
    assert visited == ["192.0.2.1", "192.0.2.2"]
