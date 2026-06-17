"""
Mock third-party vendor API.

Deliberately unreliable:
- Random latency
- Occasional 500 errors
- Per-request 429 rate limiting with Retry-After header

Configure via environment variables:
- MOCK_FAILURE_RATE   : probability of 500 response (default 0.1)
- MOCK_RATE_LIMIT_RATE: probability of 429 response (default 0.1)
- MOCK_MIN_LATENCY    : minimum response latency in seconds (default 0.1)
- MOCK_MAX_LATENCY    : maximum response latency in seconds (default 1.0)
- MOCK_RETRY_AFTER    : Retry-After header value in seconds (default 5)
"""

import asyncio
import os
import random
import time
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

app = FastAPI(title="Mock Vendor API")

# Configurable failure rates
FAILURE_RATE = float(os.getenv("MOCK_FAILURE_RATE", "0.1"))
RATE_LIMIT_RATE = float(os.getenv("MOCK_RATE_LIMIT_RATE", "0.1"))
MIN_LATENCY = float(os.getenv("MOCK_MIN_LATENCY", "0.1"))
MAX_LATENCY = float(os.getenv("MOCK_MAX_LATENCY", "1.0"))
RETRY_AFTER = int(os.getenv("MOCK_RETRY_AFTER", "5"))

# Call tracking for tests (admin endpoints)
call_log: list[dict] = []
call_counts: dict[str, int] = defaultdict(int)


class AnalyzeRequest(BaseModel):
    text: str


@app.post("/v1/analyze")
async def analyze(request: AnalyzeRequest) -> JSONResponse:
    # Simulate latency
    latency = random.uniform(MIN_LATENCY, MAX_LATENCY)
    await asyncio.sleep(latency)

    # Track call
    call_log.append({"text": request.text[:50], "ts": time.time()})
    call_counts[request.text] += 1

    # Simulate 429
    if random.random() < RATE_LIMIT_RATE:
        return JSONResponse(
            status_code=429,
            content={"error": "rate limited"},
            headers={"Retry-After": str(RETRY_AFTER)},
        )

    # Simulate 500
    if random.random() < FAILURE_RATE:
        return JSONResponse(
            status_code=500,
            content={"error": "internal server error"},
        )

    # Success
    return JSONResponse(
        status_code=200,
        content={"result": f"analyzed:{request.text[:100]}"},
    )


# --- Admin endpoints (for tests only, never used in production code) ---

@app.get("/v1/admin/calls")
async def get_calls() -> JSONResponse:
    return JSONResponse({"total": len(call_log), "calls": call_log[-100:]})


@app.get("/v1/admin/call-count")
async def get_call_count(text: str) -> JSONResponse:
    return JSONResponse({"text": text, "count": call_counts.get(text, 0)})


@app.post("/v1/admin/reset")
async def reset_calls() -> JSONResponse:
    call_log.clear()
    call_counts.clear()
    return JSONResponse({"status": "reset"})


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)