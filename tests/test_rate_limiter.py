"""
Tests for Redis token bucket rate limiter.

Covers:
- Tokens granted exactly up to capacity
- Tokens denied when bucket is empty
- Bucket refill over time
- Atomic limiting under concurrent requests
- Tenant-specific override configuration
"""
import asyncio

import pytest
from redis.asyncio import Redis

from app.config import settings
from app.rate_limiter.lua_limiter import acquire_token


@pytest.mark.asyncio
async def test_tokens_granted_up_to_capacity(redis: Redis, monkeypatch):
    """Should grant exactly capacity tokens before denying."""
    tenant = "test-tenant-capacity"
    capacity = 5
    monkeypatch.setattr(
        settings,
        "tenant_rate_limits",
        {tenant: {"capacity": capacity, "refill_rate": 0.001}},
    )

    granted = 0
    for _ in range(capacity + 5):
        if await acquire_token(redis, tenant):
            granted += 1

    assert granted == capacity


@pytest.mark.asyncio
async def test_bucket_empty_denies_request(redis: Redis, monkeypatch):
    """After exhausting the bucket, next request must be denied."""
    tenant = "test-tenant-deny"
    capacity = 3
    monkeypatch.setattr(
        settings,
        "tenant_rate_limits",
        {tenant: {"capacity": capacity, "refill_rate": 0.001}},
    )

    for _ in range(capacity):
        await acquire_token(redis, tenant)

    assert await acquire_token(redis, tenant) is False


@pytest.mark.asyncio
async def test_bucket_refills_over_time(redis: Redis, monkeypatch):
    """After waiting, tokens should be available again."""
    tenant = "test-tenant-refill"
    capacity = 2
    monkeypatch.setattr(
        settings,
        "tenant_rate_limits",
        {tenant: {"capacity": capacity, "refill_rate": 2}},
    )

    for _ in range(capacity):
        await acquire_token(redis, tenant)

    assert await acquire_token(redis, tenant) is False

    await asyncio.sleep(1.1)

    assert await acquire_token(redis, tenant) is True


@pytest.mark.asyncio
async def test_concurrent_requests_respect_limit(redis: Redis, monkeypatch):
    """Concurrent callers must not receive more tokens than capacity."""
    tenant = "test-tenant-concurrent"
    capacity = 5
    monkeypatch.setattr(
        settings,
        "tenant_rate_limits",
        {tenant: {"capacity": capacity, "refill_rate": 0.001}},
    )

    results = await asyncio.gather(*[
        acquire_token(redis, tenant)
        for _ in range(capacity * 3)
    ])

    granted = sum(1 for result in results if result)
    assert granted == capacity


@pytest.mark.asyncio
async def test_different_tenants_have_independent_buckets(redis: Redis, monkeypatch):
    """One exhausted tenant bucket must not affect another tenant."""
    capacity = 2
    monkeypatch.setattr(
        settings,
        "tenant_rate_limits",
        {
            "tenant-a-isolated": {"capacity": capacity, "refill_rate": 0.001},
            "tenant-b-isolated": {"capacity": capacity, "refill_rate": 0.001},
        },
    )

    for _ in range(capacity):
        await acquire_token(redis, "tenant-a-isolated")

    assert await acquire_token(redis, "tenant-b-isolated") is True


@pytest.mark.asyncio
async def test_tenant_specific_capacity_is_used(redis: Redis, monkeypatch):
    """Configured tenants use their own capacity/refill values."""
    tenant = "test-tenant-override"
    monkeypatch.setattr(
        settings,
        "tenant_rate_limits",
        {tenant: {"capacity": 2, "refill_rate": 0.001}},
    )

    results = [await acquire_token(redis, tenant) for _ in range(4)]

    assert results == [True, True, False, False]
