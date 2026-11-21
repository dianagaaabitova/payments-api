import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hmac import compare_digest
from typing import Annotated, Any
from uuid import UUID

from asyncpg import PostgresError  # type: ignore[import-untyped]
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, Response, Security
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, ProgrammingError, TimeoutError
from sqlalchemy.ext.asyncio import async_sessionmaker

from payments.api.http_body import PaymentRoute
from payments.contracts import PaymentAccepted, PaymentCreate, PaymentDetails, WebhookPayload
from payments.persistence.db import create_engine, is_transient_database_error
from payments.persistence.repository import IdempotencyConflict, PaymentRepository
from payments.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class ErrorResponse(BaseModel):
    detail: str


creation_router = APIRouter(route_class=PaymentRoute)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
        description="Reuse the same key and payload to recover the original creation receipt.",
    ),
]


async def require_api_key(
    key: Annotated[str | None, Security(api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    if key is None or not compare_digest(
        key.encode(), settings.api_key.get_secret_value().encode()
    ):
        raise HTTPException(
            401,
            "Invalid or missing API key",
            headers={"WWW-Authenticate": "APIKey"},
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine = create_engine(get_settings().database_url)
    app.state.payments = PaymentRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        yield
    finally:
        await engine.dispose()


def repository(request: Request) -> PaymentRepository:
    payments: PaymentRepository = request.app.state.payments
    return payments


app = FastAPI(
    title="Payments API",
    version="1.0.0",
    description="Submit a payment, track its processing, and receive a signed result webhook.",
    lifespan=lifespan,
)

Repo = Annotated[PaymentRepository, Depends(repository)]
protected: dict[int | str, dict[str, Any]] = {
    401: {"model": ErrorResponse, "description": "Missing or invalid API key"},
    503: {"model": ErrorResponse, "description": "Database temporarily unavailable"},
}


@app.exception_handler(RequestValidationError)
async def invalid_request(_request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = [
        {
            "loc": [
                part.encode("utf-8", "replace").decode() if isinstance(part, str) else part
                for part in error["loc"]
            ],
            "msg": error["msg"],
            "type": error["type"],
        }
        for error in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"detail": errors})


@app.exception_handler(OSError)
@app.exception_handler(DBAPIError)
@app.exception_handler(PostgresError)
@app.exception_handler(TimeoutError)
async def database_error(_request: Request, exc: Exception) -> JSONResponse:
    if not is_transient_database_error(exc):
        logger.error("Database operation failed", exc_info=exc)
        return JSONResponse(status_code=500, content={"detail": "Database operation failed"})
    return JSONResponse(
        status_code=503,
        content={"detail": "Database temporarily unavailable"},
        headers={"Retry-After": "2"},
    )


@creation_router.post(
    "/api/v1/payments",
    status_code=202,
    summary="Accept a payment for processing",
    dependencies=[Depends(require_api_key)],
    responses={
        **protected,
        202: {
            "description": "Original payment receipt",
            "headers": {"Location": {"schema": {"type": "string"}}},
        },
        409: {"model": ErrorResponse, "description": "Key already used with a different payload"},
        413: {"model": ErrorResponse, "description": "Request body exceeds 64 KiB"},
    },
)
async def create_payment(
    data: PaymentCreate,
    key: IdempotencyKey,
    payments: Repo,
    response: Response,
) -> PaymentAccepted:
    try:
        receipt = await payments.create(data, key)
    except IdempotencyConflict as exc:
        raise HTTPException(409, "Idempotency key already used with a different payload") from exc
    response.headers["Location"] = str(
        app.url_path_for("get_payment", payment_id=receipt.payment_id)
    )
    return receipt


app.include_router(creation_router)


@app.get(
    "/api/v1/payments/{payment_id}",
    summary="Get payment and webhook status",
    dependencies=[Depends(require_api_key)],
    responses={**protected, 404: {"model": ErrorResponse, "description": "Payment not found"}},
)
async def get_payment(payment_id: UUID, payments: Repo) -> PaymentDetails:
    payment = await payments.get(payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return payment


@app.get("/health/ready", dependencies=[Depends(require_api_key)], responses=protected)
async def readiness(payments: Repo) -> dict[str, str]:
    try:
        async with payments.sessions() as session:
            await session.execute(text("SELECT 1 FROM payments LIMIT 0"))
            await session.execute(text("SELECT 1 FROM outbox LIMIT 0"))
    except ProgrammingError as exc:
        raise HTTPException(503, "Database schema unavailable") from exc
    return {"status": "ready"}


@app.webhooks.post(
    "payment.processed",
    summary="Final payment result",
    description=(
        "The service sends this event to webhook_url. Any 2xx confirms delivery. "
        "Deduplicate by event_id. X-Webhook-Signature is sha256=<hex HMAC of the raw body>."
    ),
)
async def payment_processed(
    body: WebhookPayload,
    signature: Annotated[str, Header(alias="X-Webhook-Signature")],
    event_id: Annotated[UUID, Header(alias="X-Webhook-Id")],
) -> None:
    """OpenAPI contract for the webhook receiver."""
