import logging
import random
import asyncio

import httpx

from app.config import settings

logger = logging.getLogger(__name__)


class VendorError(Exception):
    """Base vendor error."""


class VendorRateLimited(VendorError):
    """Vendor returned 429 — includes retry_after seconds."""
    def __init__(self, retry_after: float):
        self.retry_after = retry_after


class VendorServerError(VendorError):
    """Vendor returned 5xx or timed out."""


async def call_vendor(text: str, idempotency_key: str | None = None) -> str:
    """
    Call the third-party vendor API with a single text item.

    Raises:
        VendorRateLimited  — caller should wait retry_after seconds
        VendorServerError  — caller should apply exponential backoff
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
            response = await client.post(
                f"{settings.vendor_url}/v1/analyze",
                json={"text": text},
                headers=headers,
            )
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            raise VendorServerError(f"Network error: {exc}") from exc

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", 5))
            raise VendorRateLimited(retry_after=retry_after)

        if response.status_code >= 500:
            raise VendorServerError(f"Vendor {response.status_code}: {response.text[:200]}")

        if response.status_code != 200:
            raise VendorServerError(f"Unexpected status {response.status_code}")

        return response.text


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter: 2^attempt seconds ± 20% jitter, capped at 60s."""
    base = min(2 ** attempt, 60)
    jitter = base * 0.2 * random.random()
    return base + jitter
