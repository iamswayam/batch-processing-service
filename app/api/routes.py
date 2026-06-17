import json
import logging
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_tenant_id
from app.api.schemas import (
    BatchFailureItem,
    BatchFailuresResponse,
    BatchResultItem,
    BatchResultsResponse,
    BatchStatusResponse,
    BatchSubmitRequest,
    BatchSubmitResponse,
)
from app.db.database import get_db
from app.db.models import Batch, BatchItem
from app.idempotency.manager import (
    check_idempotency,
    compute_payload_hash,
    save_idempotency_key,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def parse_result_value(value: str | None):
    if value is None:
        return ""
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def derive_batch_status(total: int, done: int, failed: int, in_progress: int) -> str:
    """Derive batch status from item counts."""
    if total == 0:
        return "completed"
    if done + failed == total:
        return "completed" if failed == 0 else "partially_failed"
    if in_progress > 0 or done > 0 or failed > 0:
        return "processing"
    return "pending"


@router.post("/batches", response_model=BatchSubmitResponse, status_code=202)
async def submit_batch(
    request: BatchSubmitRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    tenant_id: str = Depends(get_tenant_id),
    db: AsyncSession = Depends(get_db),
) -> BatchSubmitResponse:
    payload_hash = compute_payload_hash(request.items)

    # Check idempotency before doing any work
    existing_batch_id, conflict = await check_idempotency(
        db, tenant_id, idempotency_key, payload_hash
    )

    if conflict:
        raise HTTPException(status_code=409, detail="Idempotency key reused with different payload")

    if existing_batch_id:
        logger.info("Duplicate submission: tenant=%s batch=%s", tenant_id, existing_batch_id)
        return BatchSubmitResponse(batch_id=existing_batch_id)

    # New submission — create batch and items atomically
    batch_id = uuid.uuid4()
    batch = Batch(
        id=batch_id,
        tenant_id=tenant_id,
        idempotency_key=idempotency_key,
    )
    db.add(batch)
    await db.flush()

    # Insert items with stable seq index
    for seq, text in enumerate(request.items):
        item = BatchItem(
            id=uuid.uuid4(),
            batch_id=batch_id,
            seq=seq,
            payload=text,
            status="pending",
        )
        db.add(item)

    await save_idempotency_key(db, tenant_id, idempotency_key, payload_hash, batch_id)

    try:
        await db.commit()
    except IntegrityError:
        # Lost a race: another concurrent request with the same
        # (tenant_id, idempotency_key) committed first. Re-run the exact
        # same idempotency check used on the normal path — single source
        # of truth for "same payload -> return batch" vs "different
        # payload -> 409" — rather than assuming the collision means
        # success.
        await db.rollback()
        existing_batch_id, conflict = await check_idempotency(
            db, tenant_id, idempotency_key, payload_hash
        )
        if conflict:
            raise HTTPException(status_code=409, detail="Idempotency key reused with different payload")
        if existing_batch_id:
            logger.info(
                "Idempotency race resolved: tenant=%s batch=%s", tenant_id, existing_batch_id
            )
            return BatchSubmitResponse(batch_id=existing_batch_id)
        # Unexpected integrity violation unrelated to idempotency.
        # Surface for investigation.
        raise

    logger.info("Batch created: tenant=%s batch=%s items=%d", tenant_id, batch_id, len(request.items))
    return BatchSubmitResponse(batch_id=batch_id)


@router.get("/batches/{batch_id}", response_model=BatchStatusResponse)
async def get_batch_status(
    batch_id: uuid.UUID,
    tenant_id: str = Depends(get_tenant_id),
    db: AsyncSession = Depends(get_db),
) -> BatchStatusResponse:
    # Verify batch exists and belongs to tenant
    result = await db.execute(
        select(Batch).where(Batch.id == batch_id, Batch.tenant_id == tenant_id)
    )
    batch = result.scalar_one_or_none()
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Derive status from item counts
    counts = await db.execute(
        select(BatchItem.status, func.count().label("count"))
        .where(BatchItem.batch_id == batch_id)
        .group_by(BatchItem.status)
    )
    status_counts = {row.status: row.count for row in counts}

    total = sum(status_counts.values())
    done = status_counts.get("done", 0)
    failed = status_counts.get("failed", 0)
    in_progress = status_counts.get("in_progress", 0)

    status = derive_batch_status(total, done, failed, in_progress)

    return BatchStatusResponse(
        batch_id=batch_id,
        status=status,
        total=total,
        done=done,
        failed=failed,
    )


@router.get("/batches/{batch_id}/results", response_model=BatchResultsResponse)
async def get_batch_results(
    batch_id: uuid.UUID,
    tenant_id: str = Depends(get_tenant_id),
    db: AsyncSession = Depends(get_db),
) -> BatchResultsResponse:
    # Verify batch belongs to tenant
    result = await db.execute(
        select(Batch).where(Batch.id == batch_id, Batch.tenant_id == tenant_id)
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Return only completed items — works mid-run
    items_result = await db.execute(
        select(BatchItem.seq, BatchItem.result)
        .where(BatchItem.batch_id == batch_id, BatchItem.status == "done")
        .order_by(BatchItem.seq)
    )

    return BatchResultsResponse(
        batch_id=batch_id,
        items=[
            BatchResultItem(seq=row.seq, result=parse_result_value(row.result))
            for row in items_result
        ],
    )


@router.get("/batches/{batch_id}/failures", response_model=BatchFailuresResponse)
async def get_batch_failures(
    batch_id: uuid.UUID,
    tenant_id: str = Depends(get_tenant_id),
    db: AsyncSession = Depends(get_db),
) -> BatchFailuresResponse:
    # Verify batch belongs to tenant
    result = await db.execute(
        select(Batch).where(Batch.id == batch_id, Batch.tenant_id == tenant_id)
    )
    if result.scalar_one_or_none() is None:
        raise HTTPException(status_code=404, detail="Batch not found")

    # Return only permanently failed items
    items_result = await db.execute(
        select(BatchItem.seq, BatchItem.attempt_count, BatchItem.error_message)
        .where(BatchItem.batch_id == batch_id, BatchItem.status == "failed")
        .order_by(BatchItem.seq)
    )

    return BatchFailuresResponse(
        batch_id=batch_id,
        items=[
            BatchFailureItem(
                seq=row.seq,
                attempt_count=row.attempt_count,
                last_error=row.error_message,
            )
            for row in items_result
        ],
    )
