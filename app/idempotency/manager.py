import hashlib
import json
import logging
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Batch, IdempotencyKey

logger = logging.getLogger(__name__)


def compute_payload_hash(items: list[str]) -> str:
    """Stable SHA-256 hash of the submitted items."""
    payload = json.dumps(items, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


async def check_idempotency(
    db: AsyncSession,
    tenant_id: str,
    idempotency_key: str,
    payload_hash: str,
) -> tuple[str | None, bool]:
    """
    Returns (existing_batch_id, conflict).

    - (batch_id, False) -> duplicate submission, return existing batch_id
    - (None, True)      -> same key, different payload -> 409 Conflict
    - (None, False)     -> new submission, proceed
    """
    result = await db.execute(
        select(IdempotencyKey).where(
            IdempotencyKey.tenant_id == tenant_id,
            IdempotencyKey.idempotency_key == idempotency_key,
        )
    )
    record = result.scalar_one_or_none()

    if record is None:
        return None, False

    if record.payload_hash != payload_hash:
        logger.warning("Idempotency conflict: tenant=%s", tenant_id)
        return None, True

    return str(record.batch_id), False


async def save_idempotency_key(
    db: AsyncSession,
    tenant_id: str,
    idempotency_key: str,
    payload_hash: str,
    batch_id: uuid.UUID,
) -> None:
    """Persist the idempotency record after a new batch is created."""
    record = IdempotencyKey(
        tenant_id=tenant_id,
        idempotency_key=idempotency_key,
        payload_hash=payload_hash,
        batch_id=batch_id,
    )
    db.add(record)
    await db.flush()
