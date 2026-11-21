from decimal import Decimal

import pytest
from pydantic import ValidationError

from payments.api import app
from payments.contracts import PaymentCreate
from payments.delivery.webhook import is_public_address


def test_payment_contract_preserves_decimal_and_exposes_auth() -> None:
    payment = PaymentCreate.model_validate(
        {
            "amount": "1234.5",
            "currency": "RUB",
            "description": " order ",
            "webhook_url": "https://example.com/callback",
        }
    )
    assert payment.amount == Decimal("1234.50")
    assert payment.description == "order"
    assert payment.model_dump(mode="json")["amount"] == "1234.50"

    schema = app.openapi()
    assert {"APIKeyHeader": []} in schema["paths"]["/api/v1/payments"]["post"]["security"]
    assert "payment.processed" in schema["webhooks"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("amount", "0"),
        ("amount", "3.141"),
        ("amount", "99999999999999999"),
        ("currency", "JPY"),
        ("description", "bad\x00text"),
        ("metadata", {"key": [float("nan")]}),
        ("webhook_url", "https://user:pass@example.com/"),
        ("webhook_url", "file:///tmp/payment"),
    ],
)
def test_invalid_payment_is_rejected(field: str, value: object) -> None:
    data: dict[str, object] = {
        "amount": "19.00",
        "currency": "USD",
        "description": "Order",
        "webhook_url": "https://example.com/callback",
    }
    data[field] = value
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(data)


@pytest.mark.parametrize(
    "address",
    ["127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "::ffff:127.0.0.1", "ff02::1"],
)
def test_internal_and_special_addresses_are_denied(address: str) -> None:
    assert not is_public_address(address)
    assert is_public_address("8.8.8.8")
