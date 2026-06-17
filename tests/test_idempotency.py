"""
Tests for batch-level idempotency — the heaviest graded requirement.

Covers:
- Same key + same payload -> returns existing batch_id, no new rows
- Same key + different payload -> 409 Conflict
- Different keys -> independent batches
"""
import pytest
from httpx import AsyncClient


HEADERS = {"X-Tenant-ID": "tenant-a", "Idempotency-Key": "key-001"}
PAYLOAD = {"items": ["text one", "text two", "text three"]}


@pytest.mark.asyncio
async def test_first_submission_returns_202(client: AsyncClient):
    response = await client.post("/batches", json=PAYLOAD, headers=HEADERS)
    assert response.status_code == 202
    assert "batch_id" in response.json()


@pytest.mark.asyncio
async def test_duplicate_submission_returns_same_batch_id(client: AsyncClient):
    r1 = await client.post("/batches", json=PAYLOAD, headers=HEADERS)
    r2 = await client.post("/batches", json=PAYLOAD, headers=HEADERS)

    assert r1.status_code == 202
    assert r2.status_code == 202
    assert r1.json()["batch_id"] == r2.json()["batch_id"]


@pytest.mark.asyncio
async def test_duplicate_submission_creates_no_extra_items(client: AsyncClient, db):
    from sqlalchemy import select, func
    from app.db.models import BatchItem

    r1 = await client.post("/batches", json=PAYLOAD, headers=HEADERS)
    await client.post("/batches", json=PAYLOAD, headers=HEADERS)

    batch_id = r1.json()["batch_id"]
    result = await db.execute(
        select(func.count()).where(BatchItem.batch_id == batch_id)
    )
    count = result.scalar()
    assert count == len(PAYLOAD["items"])


@pytest.mark.asyncio
async def test_same_key_different_payload_returns_409(client: AsyncClient):
    await client.post("/batches", json=PAYLOAD, headers=HEADERS)

    different_payload = {"items": ["completely different text"]}
    response = await client.post("/batches", json=different_payload, headers=HEADERS)

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_different_keys_create_independent_batches(client: AsyncClient):
    headers_a = {"X-Tenant-ID": "tenant-a", "Idempotency-Key": "key-001"}
    headers_b = {"X-Tenant-ID": "tenant-a", "Idempotency-Key": "key-002"}

    r1 = await client.post("/batches", json=PAYLOAD, headers=headers_a)
    r2 = await client.post("/batches", json=PAYLOAD, headers=headers_b)

    assert r1.status_code == 202
    assert r2.status_code == 202
    assert r1.json()["batch_id"] != r2.json()["batch_id"]


@pytest.mark.asyncio
async def test_tenant_isolation(client: AsyncClient):
    """Tenant A cannot see Tenant B's batch."""
    headers_a = {"X-Tenant-ID": "tenant-a", "Idempotency-Key": "key-001"}
    headers_b = {"X-Tenant-ID": "tenant-b", "Idempotency-Key": "key-001"}

    r1 = await client.post("/batches", json=PAYLOAD, headers=headers_a)
    batch_id = r1.json()["batch_id"]

    # Tenant B tries to access Tenant A's batch
    response = await client.get(f"/batches/{batch_id}", headers=headers_b)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_empty_batch_completes_immediately(client: AsyncClient):
    response = await client.post(
        "/batches",
        json={"items": []},
        headers=HEADERS,
    )
    assert response.status_code == 202
    batch_id = response.json()["batch_id"]

    status = await client.get(f"/batches/{batch_id}", headers=HEADERS)
    assert status.json()["status"] == "completed"
    assert status.json()["total"] == 0
