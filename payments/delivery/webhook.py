import asyncio
import hmac
import socket
from ipaddress import IPv6Address, ip_address
from typing import Protocol

import httpx

from payments.contracts import WebhookPayload


class WebhookError(Exception):
    """Carries a safe diagnostic code, without the URL or response body."""


class WebhookSender(Protocol):
    async def send(self, url: str, event: WebhookPayload) -> None: ...


def is_public_address(address: str) -> bool:
    ip = ip_address(address)
    if isinstance(ip, IPv6Address):
        ip = ip.ipv4_mapped or ip
    return ip.is_global and not ip.is_multicast and not ip.is_reserved


class HttpWebhookSender:
    def __init__(
        self,
        secret: str,
        timeout: float,
        allowed_hosts: frozenset[str],
    ) -> None:
        self.secret = secret.encode()
        self.timeout = timeout
        self.allowed_hosts = frozenset(host.lower().rstrip(".") for host in allowed_hosts)

    async def send(self, url: str, event: WebhookPayload) -> None:
        target = httpx.URL(url)
        body = event.model_dump_json().encode()
        signature = hmac.digest(self.secret, body, "sha256").hex()
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": f"sha256={signature}",
            "X-Webhook-Id": str(event.event_id),
            "Host": target.netloc.decode("ascii"),
        }

        try:
            async with asyncio.timeout(self.timeout), httpx.AsyncClient(trust_env=False) as client:
                host = target.raw_host.decode("ascii")
                endpoints = await self._resolve(host, target.port)
                timeout = httpx.Timeout(self.timeout, connect=self.timeout / len(endpoints))
                for index, endpoint in enumerate(endpoints):
                    try:
                        # Preserve HTTP and TLS identity while connecting to the checked IP.
                        async with client.stream(
                            "POST",
                            target.copy_with(host=endpoint),
                            headers=headers,
                            content=body,
                            extensions={"sni_hostname": host.rstrip(".")},
                            follow_redirects=False,
                            timeout=timeout,
                        ) as response:
                            if not response.is_success:
                                raise WebhookError(f"http_{response.status_code}")
                        return
                    except (httpx.ConnectError, httpx.ConnectTimeout):
                        if index == len(endpoints) - 1:
                            raise
        except socket.gaierror as exc:
            raise WebhookError("dns_error") from exc
        except (TimeoutError, httpx.TimeoutException) as exc:
            raise WebhookError("timeout") from exc
        except (httpx.HTTPError, OSError) as exc:
            raise WebhookError("http_transport_error") from exc

    async def _resolve(self, host: str, port: int | None) -> list[str]:
        addresses = await asyncio.get_running_loop().getaddrinfo(
            host,
            port,
            type=socket.SOCK_STREAM,
        )
        endpoints = sorted({str(address[0]) for _, _, _, _, address in addresses})
        if not endpoints:
            raise WebhookError("dns_error")
        if host.rstrip(".").lower() not in self.allowed_hosts and not all(
            is_public_address(address) for address in endpoints
        ):
            raise WebhookError("destination_denied")
        return endpoints
