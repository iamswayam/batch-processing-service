import uuid
from typing import Any

from pydantic import BaseModel


# ---------- Request ----------

class BatchSubmitRequest(BaseModel):
    items: list[str]


# ---------- Response: POST /batches ----------

class BatchSubmitResponse(BaseModel):
    batch_id: uuid.UUID


# ---------- Response: GET /batches/{batch_id} ----------

class BatchStatusResponse(BaseModel):
    batch_id: uuid.UUID
    status: str          # pending | processing | completed | partially_failed
    total: int
    done: int
    failed: int


# ---------- Response: GET /batches/{batch_id}/results ----------

class BatchResultItem(BaseModel):
    seq: int
    result: Any


class BatchResultsResponse(BaseModel):
    batch_id: uuid.UUID
    items: list[BatchResultItem]


# ---------- Response: GET /batches/{batch_id}/failures ----------

class BatchFailureItem(BaseModel):
    seq: int
    attempt_count: int
    last_error: str | None


class BatchFailuresResponse(BaseModel):
    batch_id: uuid.UUID
    items: list[BatchFailureItem]
