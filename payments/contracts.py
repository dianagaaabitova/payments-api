import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Self
from uuid import UUID

from pydantic import (
    AfterValidator,
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StringConstraints,
    field_validator,
)


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class WebhookStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    EXHAUSTED = "exhausted"


class EventKind(StrEnum):
    PROCESS = "process"
    DEAD_LETTER = "dead_letter"


MAX_ATTEMPTS = 3


def validate_text(value: str) -> str:
    if "\x00" in value:
        raise ValueError("NUL characters are not supported")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("Invalid Unicode") from exc
    return value


def validate_json(value: JsonValue, depth: int = 0) -> None:
    if depth > 20:
        raise ValueError("Metadata nesting exceeds 20 levels")
    if isinstance(value, str):
        validate_text(value)
    elif isinstance(value, dict):
        for key, item in value.items():
            validate_text(key)
            validate_json(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            validate_json(item, depth + 1)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Currency
    description: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=1000),
        AfterValidator(validate_text),
    ]
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: Annotated[AnyHttpUrl, Field(max_length=2048)]

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))

    @field_validator("metadata")
    @classmethod
    def limit_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        validate_json(value)
        serialized = json.dumps(value, ensure_ascii=False, allow_nan=False)
        if len(serialized.encode()) > 16384:
            raise ValueError("Metadata exceeds 16 KiB")
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None or value.fragment is not None:
            raise ValueError("Webhook URL cannot contain credentials or a fragment")
        if len(str(value)) > 2048:
            raise ValueError("Webhook URL exceeds 2048 characters after normalization")
        return value


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
    processed_at: datetime | None
    webhook_status: WebhookStatus
    webhook_attempts: int
    gateway_failures: int


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    payment_id: UUID
    attempt: Annotated[int, Field(ge=1, le=MAX_ATTEMPTS, strict=True)]
    gateway_attempt: Annotated[int, Field(ge=1, le=MAX_ATTEMPTS, strict=True)] = 1


class WebhookPayload(BaseModel):
    event_id: UUID
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime

    @classmethod
    def from_payment(cls, payment: PaymentDetails) -> Self:
        assert payment.processed_at is not None
        return cls(
            event_id=payment.payment_id,
            payment_id=payment.payment_id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
