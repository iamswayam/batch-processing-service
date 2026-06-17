import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.adapters.vendor_client import VendorRateLimited, VendorServerError, backoff_seconds, call_vendor
from app.config import settings
from app.db.models import Batch, BatchItem
from app.rate_limiter.lua_limiter import acquire_token

logger = logging.getLogger(__name__)

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def claim_pending_items(db: AsyncSession) -> list[tuple[BatchItem, str, datetime]]:
    """
    Claim a chunk of pending items using SELECT FOR UPDATE SKIP LOCKED.
    Sets status to in_progress and stamps lease_expires_at.

    Joins to `batches` to fetch tenant_id without denormalizing it onto
    BatchItem — schema stays normalized, one indexed join on a small
    (chunk_size-bounded) result set. `of=BatchItem` ensures only the
    queue rows are locked, not the parent batch row.
    """
    now = datetime.now(timezone.utc)
    lease_until = now + timedelta(seconds=settings.worker_lease_timeout)

    result = await db.execute(
        select(BatchItem, Batch.tenant_id)
        .join(Batch, BatchItem.batch_id == Batch.id)
        .where(
            BatchItem.status == "pending",
            (BatchItem.next_retry_at == None) | (BatchItem.next_retry_at <= now),
        )
        .limit(settings.worker_chunk_size)
        .with_for_update(of=BatchItem, skip_locked=True)
    )
    rows = result.all()  # list of Row(BatchItem, tenant_id)

    if not rows:
        return []

    item_ids = [row[0].id for row in rows]
    await db.execute(
        update(BatchItem)
        .where(BatchItem.id.in_(item_ids))
        .values(status="in_progress", lease_expires_at=lease_until)
    )
    await db.commit()
    for row in rows:
        row[0].lease_expires_at = lease_until
    return [(row[0], row[1], lease_until) for row in rows]


async def item_lease_owner(
    db: AsyncSession, item_id: uuid.UUID, claimed_lease: datetime
) -> bool:
    """Return True if this worker still holds the in_progress lease it claimed."""
    result = await db.execute(
        select(BatchItem.id).where(
            BatchItem.id == item_id,
            BatchItem.status == "in_progress",
            BatchItem.lease_expires_at == claimed_lease,
        )
    )
    return result.scalar_one_or_none() is not None


def _lease_owner_where(item_id: uuid.UUID, claimed_lease: datetime):
    return (
        BatchItem.id == item_id,
        BatchItem.status == "in_progress",
        BatchItem.lease_expires_at == claimed_lease,
    )


async def process_item(
    item: BatchItem,
    tenant_id: str,
    claimed_lease: datetime,
    redis: Redis,
    semaphore: asyncio.Semaphore,
) -> None:
    """Process a single item: acquire rate limit token, call vendor, persist result."""
    async with semaphore:
        async with AsyncSessionLocal() as db:
            # Acquire rate limit token before calling vendor.
            # Keyed by tenant_id (not batch_id) so the bucket is shared
            # across all of a tenant's concurrent batches, per spec.
            while not await acquire_token(redis, tenant_id):
                logger.info("Waiting for rate limit token: item=%s tenant=%s", item.id, tenant_id)
                await asyncio.sleep(1)

            if not await item_lease_owner(db, item.id, claimed_lease):
                logger.warning("Lost lease before vendor call, skipping: item=%s", item.id)
                return

            try:
                vendor_idempotency_key = f"{item.batch_id}:{item.seq}"
                result = await call_vendor(item.payload, idempotency_key=vendor_idempotency_key)
                if await mark_done(db, item.id, claimed_lease, result):
                    logger.info("Item done: item=%s", item.id)
                else:
                    logger.warning("Lost lease before mark_done, skipping: item=%s", item.id)

            except VendorRateLimited as exc:
                # Respect Retry-After — return to pending, don't count as failure attempt
                if await mark_retry(db, item.id, claimed_lease, item.attempt_count, exc.retry_after):
                    logger.info("Item rate limited by vendor, retry after %.1fs: item=%s", exc.retry_after, item.id)
                else:
                    logger.warning("Lost lease before mark_retry, skipping: item=%s", item.id)

            except VendorServerError as exc:
                new_attempt = item.attempt_count + 1
                if new_attempt >= settings.max_retry_count:
                    if await mark_failed(db, item.id, claimed_lease, new_attempt, str(exc)):
                        logger.warning("Item permanently failed: item=%s error=%s", item.id, exc)
                    else:
                        logger.warning("Lost lease before mark_failed, skipping: item=%s", item.id)
                else:
                    delay = backoff_seconds(new_attempt)
                    if await mark_retry(db, item.id, claimed_lease, new_attempt, delay):
                        logger.info("Item retry %d scheduled in %.1fs: item=%s", new_attempt, delay, item.id)
                    else:
                        logger.warning("Lost lease before mark_retry, skipping: item=%s", item.id)


async def mark_done(
    db: AsyncSession, item_id: uuid.UUID, claimed_lease: datetime, result: str
) -> bool:
    update_result = await db.execute(
        update(BatchItem)
        .where(*_lease_owner_where(item_id, claimed_lease))
        .values(status="done", result=result, lease_expires_at=None)
    )
    await db.commit()
    return update_result.rowcount > 0


async def mark_failed(
    db: AsyncSession, item_id: uuid.UUID, claimed_lease: datetime, attempt_count: int, error: str
) -> bool:
    update_result = await db.execute(
        update(BatchItem)
        .where(*_lease_owner_where(item_id, claimed_lease))
        .values(status="failed", attempt_count=attempt_count, error_message=error, lease_expires_at=None)
    )
    await db.commit()
    return update_result.rowcount > 0


async def mark_retry(
    db: AsyncSession, item_id: uuid.UUID, claimed_lease: datetime, attempt_count: int, delay: float
) -> bool:
    next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)
    update_result = await db.execute(
        update(BatchItem)
        .where(*_lease_owner_where(item_id, claimed_lease))
        .values(
            status="pending",
            attempt_count=attempt_count,
            next_retry_at=next_retry,
            lease_expires_at=None,
        )
    )
    await db.commit()
    return update_result.rowcount > 0


async def sweep_stale_leases(db: AsyncSession) -> None:
    """Reset in_progress items whose lease has expired back to pending."""
    now = datetime.now(timezone.utc)
    result = await db.execute(
        update(BatchItem)
        .where(BatchItem.status == "in_progress", BatchItem.lease_expires_at <= now)
        .values(status="pending", lease_expires_at=None)
    )
    await db.commit()
    if result.rowcount:
        logger.warning("Sweeper reset %d stale items", result.rowcount)


async def run_worker() -> None:
    """Main worker loop — claims items, processes concurrently, sweeps stale leases."""
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    logger.info("Worker started")

    while True:
        async with AsyncSessionLocal() as db:
            await sweep_stale_leases(db)
            items = await claim_pending_items(db)

        if items:
            await asyncio.gather(*[
                process_item(item, tenant_id, claimed_lease, redis, semaphore)
                for item, tenant_id, claimed_lease in items
            ])
        else:
            await asyncio.sleep(settings.worker_poll_interval)


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker())
