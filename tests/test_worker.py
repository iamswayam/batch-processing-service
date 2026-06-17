"""
Tests for worker behavior — restart recovery and partial failure visibility.

Covers:
- Stale lease sweeper resets in_progress items
- Partial failure: done items queryable while some items failed
- Failures endpoint shows item id, attempt count, last error
- Results endpoint works mid-run (returns done items only)
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Batch, BatchItem
from app.worker.engine import claim_pending_items, mark_done, sweep_stale_leases


async def _create_batch_with_items(
    db: AsyncSession,
    tenant_id: str,
    item_count: int,
) -> uuid.UUID:
    """Helper: insert a batch and items directly into DB."""
    batch_id = uuid.uuid4()
    batch = Batch(
        id=batch_id,
        tenant_id=tenant_id,
        idempotency_key=str(uuid.uuid4()),
    )
    db.add(batch)
    await db.flush()

    for seq in range(item_count):
        db.add(BatchItem(
            id=uuid.uuid4(),
            batch_id=batch_id,
            seq=seq,
            payload=f"text item {seq}",
            status="pending",
        ))

    await db.commit()
    return batch_id


@pytest.mark.asyncio
async def test_claim_pending_items_returns_tenant_id(db: AsyncSession):
    """Claiming work carries tenant_id into the worker rate-limit path."""
    batch_id = await _create_batch_with_items(db, "tenant-claim", 2)

    claimed = await claim_pending_items(db)

    assert len(claimed) == 2
    assert all(tenant_id == "tenant-claim" for _, tenant_id, _ in claimed)
    assert all(claimed_lease is not None for _, _, claimed_lease in claimed)

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    items = result.scalars().all()
    assert all(item.status == "in_progress" for item in items)


@pytest.mark.asyncio
async def test_sweeper_resets_stale_in_progress_items(db: AsyncSession):
    """
    Items stuck in_progress past their lease_expires_at must be reset to pending.
    This is the crash recovery mechanism.
    """
    batch_id = await _create_batch_with_items(db, "tenant-sweep", 3)

    # Simulate worker crash: mark items in_progress with expired lease
    expired_lease = datetime.now(timezone.utc) - timedelta(seconds=120)
    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    items = result.scalars().all()

    for item in items:
        item.status = "in_progress"
        item.lease_expires_at = expired_lease

    await db.commit()

    # Run sweeper
    await sweep_stale_leases(db)

    # All items should be back to pending
    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    reset_items = result.scalars().all()

    assert all(item.status == "pending" for item in reset_items)
    assert all(item.lease_expires_at is None for item in reset_items)


@pytest.mark.asyncio
async def test_sweeper_ignores_valid_leases(db: AsyncSession):
    """Items with a future lease_expires_at must NOT be reset."""
    batch_id = await _create_batch_with_items(db, "tenant-valid-lease", 2)

    future_lease = datetime.now(timezone.utc) + timedelta(seconds=300)
    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    items = result.scalars().all()

    for item in items:
        item.status = "in_progress"
        item.lease_expires_at = future_lease

    await db.commit()

    await sweep_stale_leases(db)

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    still_items = result.scalars().all()

    assert all(item.status == "in_progress" for item in still_items)


@pytest.mark.asyncio
async def test_mark_done_rejects_stale_lease(db: AsyncSession):
    """mark_done must not overwrite state when the worker no longer owns the lease."""
    batch_id = await _create_batch_with_items(db, "tenant-stale-lease", 1)

    claimed = await claim_pending_items(db)
    item, _, stale_lease = claimed[0]

    # Another worker reclaimed the item with a new lease.
    new_lease = datetime.now(timezone.utc) + timedelta(seconds=300)
    result = await db.execute(select(BatchItem).where(BatchItem.id == item.id))
    db_item = result.scalar_one()
    db_item.status = "done"
    db_item.result = "peer-result"
    db_item.lease_expires_at = new_lease
    await db.commit()

    applied = await mark_done(db, item.id, stale_lease, "stale-worker-result")
    assert applied is False

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    final_item = result.scalar_one()
    assert final_item.status == "done"
    assert final_item.result == "peer-result"


@pytest.mark.asyncio
async def test_partial_failure_status(client: AsyncClient, db: AsyncSession):
    """
    A batch where some items completed and some permanently failed
    should show status=partially_failed.
    """
    batch_id = await _create_batch_with_items(db, "tenant-partial", 4)
    headers = {"X-Tenant-ID": "tenant-partial"}

    # Mark 2 done, 2 failed directly in DB
    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    items = result.scalars().all()

    items[0].status = "done"
    items[0].result = "result-0"
    items[1].status = "done"
    items[1].result = "result-1"
    items[2].status = "failed"
    items[2].error_message = "timeout"
    items[2].attempt_count = 5
    items[3].status = "failed"
    items[3].error_message = "server error"
    items[3].attempt_count = 5

    await db.commit()

    response = await client.get(f"/batches/{batch_id}", headers=headers)
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "partially_failed"
    assert data["total"] == 4
    assert data["done"] == 2
    assert data["failed"] == 2


@pytest.mark.asyncio
async def test_results_endpoint_returns_done_items_only(client: AsyncClient, db: AsyncSession):
    """Results endpoint returns only completed items — works mid-run."""
    batch_id = await _create_batch_with_items(db, "tenant-results", 3)
    headers = {"X-Tenant-ID": "tenant-results"}

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    items = sorted(result.scalars().all(), key=lambda x: x.seq)

    # Only mark first item done, leave others pending
    items[0].status = "done"
    items[0].result = "analyzed:text item 0"

    await db.commit()

    response = await client.get(f"/batches/{batch_id}/results", headers=headers)
    assert response.status_code == 200

    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["seq"] == 0
    assert data["items"][0]["result"] == "analyzed:text item 0"


@pytest.mark.asyncio
async def test_results_endpoint_parses_json_result_values(client: AsyncClient, db: AsyncSession):
    """JSON-encoded stored results are returned as JSON values."""
    batch_id = await _create_batch_with_items(db, "tenant-json-results", 1)
    headers = {"X-Tenant-ID": "tenant-json-results"}

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    item = result.scalar_one()
    item.status = "done"
    item.result = '{"result":"analyzed:item1"}'

    await db.commit()

    response = await client.get(f"/batches/{batch_id}/results", headers=headers)

    assert response.status_code == 200
    assert response.json()["items"] == [
        {"seq": 0, "result": {"result": "analyzed:item1"}}
    ]


@pytest.mark.asyncio
async def test_results_endpoint_keeps_non_json_result_values(client: AsyncClient, db: AsyncSession):
    """Plain string stored results remain strings for backward compatibility."""
    batch_id = await _create_batch_with_items(db, "tenant-plain-results", 1)
    headers = {"X-Tenant-ID": "tenant-plain-results"}

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    item = result.scalar_one()
    item.status = "done"
    item.result = "plain text"

    await db.commit()

    response = await client.get(f"/batches/{batch_id}/results", headers=headers)

    assert response.status_code == 200
    assert response.json()["items"] == [
        {"seq": 0, "result": "plain text"}
    ]


@pytest.mark.asyncio
async def test_failures_endpoint_shows_error_details(client: AsyncClient, db: AsyncSession):
    """Failures endpoint must include seq, attempt_count, and last_error."""
    batch_id = await _create_batch_with_items(db, "tenant-failures", 2)
    headers = {"X-Tenant-ID": "tenant-failures"}

    result = await db.execute(select(BatchItem).where(BatchItem.batch_id == batch_id))
    items = sorted(result.scalars().all(), key=lambda x: x.seq)

    items[0].status = "failed"
    items[0].attempt_count = 5
    items[0].error_message = "Vendor 500: internal server error"

    await db.commit()

    response = await client.get(f"/batches/{batch_id}/failures", headers=headers)
    assert response.status_code == 200

    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["seq"] == 0
    assert data["items"][0]["attempt_count"] == 5
    assert data["items"][0]["last_error"] == "Vendor 500: internal server error"
