import json
from collections.abc import Callable, Coroutine
from typing import Any, Self

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

MAX_BODY_BYTES = 64 * 1024


class JsonFloat(float):
    token: str

    def __new__(cls, token: str) -> Self:
        value = super().__new__(cls, token)
        value.token = token
        return value


class PaymentRequest(Request):
    _payment_body: bytes | None = None

    async def body(self) -> bytes:
        if self._payment_body is None:
            body = bytearray()
            async for chunk in self.stream():
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    raise HTTPException(413, "Request body exceeds 64 KiB")
                body.extend(chunk)
            self._payment_body = bytes(body)
        return self._payment_body

    async def json(self) -> Any:
        data = json.loads(await self.body(), parse_float=JsonFloat)
        # Preserve the amount's JSON token before it can lose precision as a float.
        if isinstance(data, dict) and isinstance(amount := data.get("amount"), JsonFloat):
            data["amount"] = amount.token
        return data


class PaymentRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def handle(request: Request) -> Response:
            return await handler(PaymentRequest(request.scope, request.receive))

        return handle
