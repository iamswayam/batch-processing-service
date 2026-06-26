# Batch Processing Service

Production-style multi-tenant asynchronous batch processing service built with FastAPI, PostgreSQL, Redis, and asyncio workers. The system demonstrates durable work queues, strict idempotency, distributed rate limiting, lease-based recovery, and resilient processing of unreliable third-party APIs.

## Architecture

The service is organized into five independent execution layers. Each layer owns a single responsibility, making the system easier to reason about, test, maintain, and scale while ensuring safe concurrent processing.

### High-Level System Architecture

![System Architecture](assets/architecture/system-architecture.png)

## Technology Stack

The following diagram illustrates how the major technologies interact throughout the request lifecycle.

![Technology Stack](assets/architecture/technology-stack.png)

## Architecture Walkthrough

## Super Layer 1 – API & Request Processing

This layer is responsible for receiving client requests, validating input, identifying tenants, enforcing request constraints, and routing work into the system.

**Responsibilities**

- FastAPI request routing
- Request validation
- Tenant identification
- Request size protection
- Response serialization
- Health endpoint

![Super Layer 1](assets/architecture/super-layer-1-api-request-processing.png)

### Request Flow

The following diagram shows how an incoming request flows through the API layer before entering the persistence layer.

![Layer 1 Data Flow](assets/architecture/super-layer-1-data-flow.png)

## Super Layer 2 – Admission Control & Idempotent Persistence

This layer guarantees duplicate submissions never create duplicate work by combining deterministic payload hashing, idempotency validation, conflict detection, and atomic persistence.

**Responsibilities**

- Payload hashing
- Duplicate detection
- Conflict detection
- Batch idempotency
- Atomic persistence
- Durable batch creation

![Super Layer 2](assets/architecture/super-layer-2-admission-control-idempotency.png)

### Execution Flow

The following diagram illustrates duplicate detection, payload verification, and atomic persistence inside a single transaction.

![Layer 2 Data Flow](assets/architecture/super-layer-2-data-flow.png)

## Super Layer 3 – Async Worker Coordination

This layer coordinates asynchronous processing while ensuring that each item is processed safely through durable leases, ownership validation, retries, and crash recovery.

**Responsibilities**

- Worker scheduling
- Pending item claiming
- Lease ownership validation
- Retry scheduling
- Crash recovery
- Durable state transitions

![Super Layer 3](assets/architecture/super-layer-3-worker-coordination.png)

### Execution Flow

The following diagram illustrates worker claiming, lease validation, retry scheduling, and durable state transitions.

![Layer 3 Data Flow](assets/architecture/super-layer-3-data-flow.png)

## Super Layer 4 – Distributed Rate Limiting

The rate limiting layer provides fair request scheduling across tenants using a Redis-backed token bucket implemented with an atomic Lua script.

**Responsibilities**

- Per-tenant rate limiting
- Redis token bucket
- Atomic token acquisition
- Configurable refill rates
- Shared limits across worker processes

![Super Layer 4](assets/architecture/super-layer-4-distributed-rate-limiting.png)

## Super Layer 5 – External Vendor Integration

The vendor integration layer encapsulates all communication with the third-party API, including retries, exponential backoff, Retry-After handling, and vendor idempotency headers.

**Responsibilities**

- Vendor API communication
- Retry handling
- Exponential backoff
- Error classification
- Vendor idempotency support

![Super Layer 5](assets/architecture/super-layer-5-vendor-integration.png)

## Architecture Principles

The implementation intentionally separates API admission, idempotency, persistence, worker coordination, distributed rate limiting, and vendor integration into independent architectural layers.

This separation keeps each concern independently testable while improving maintainability, resilience, and reasoning about concurrent execution.

---

## Quick Start

```bash
cp .env.example .env
docker compose up --build
```

The API runs at:

```text
http://localhost:8000
```

Health check:

```bash
curl http://localhost:8000/healthz
```

## Services

| Service    |   Port | Purpose                                    |
| ---------- | -----: | ------------------------------------------ |
| `web`      | `8000` | FastAPI application                        |
| `worker`   |      - | Async batch processor                      |
| `postgres` | `5432` | Durable batch, item, and idempotency state |
| `redis`    | `6379` | Distributed token bucket rate limiter      |
| `vendor`   | `8080` | Mock third-party text analysis API         |

## API

Every request is scoped by `X-Tenant-ID`. `POST /batches` also requires `Idempotency-Key`.

### Submit A Batch

```bash
curl -X POST http://localhost:8000/batches \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-a" \
  -H "Idempotency-Key: batch-key-001" \
  -d '{"items": ["text one", "text two", "text three"]}'
```

Response:

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### Check Status

```bash
curl http://localhost:8000/batches/{batch_id} \
  -H "X-Tenant-ID: tenant-a"
```

Response:

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "total": 3,
  "done": 1,
  "failed": 0
}
```

Status values:

```text
pending | processing | completed | partially_failed
```

### Get Successful Results

Works while the batch is still running and after it completes.

```bash
curl http://localhost:8000/batches/{batch_id}/results \
  -H "X-Tenant-ID: tenant-a"
```

Response:

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    { "seq": 0, "result": { "result": "analyzed:text one" } }
  ]
}
```

If a stored result is valid JSON, the endpoint returns the parsed JSON value. Non-JSON result values are returned as their original strings for backward compatibility.

### Get Failures

```bash
curl http://localhost:8000/batches/{batch_id}/failures \
  -H "X-Tenant-ID: tenant-a"
```

Response:

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    {
      "seq": 2,
      "attempt_count": 5,
      "last_error": "Vendor 500: internal server error"
    }
  ]
}
```

## Idempotency

Batch submission is idempotent per tenant:

| Scenario                                   | Result                                                   |
| ------------------------------------------ | -------------------------------------------------------- |
| Same `Idempotency-Key` + same payload      | Returns the original `batch_id`; no new work is enqueued |
| Same `Idempotency-Key` + different payload | Returns `409 Conflict`                                   |
| Different key                              | Creates a new batch                                      |

Item processing uses durable row state, `SELECT ... FOR UPDATE SKIP LOCKED`, time-based leases with sweeper recovery, lease ownership validation, and stable item sequence numbers to prevent concurrent duplicate processing. Vendor calls include a deterministic item idempotency key header:

```text
Idempotency-Key: {batch_id}:{seq}
```

That header is the client-side hook for vendors that support idempotency. Combined with durable leases and ownership validation, the local system prevents stale workers from overwriting completed work. Strict external exactly-once execution still depends on vendor support for idempotency.

## Design Notes

* PostgreSQL is the durable work queue and source of truth.
* Workers claim pending rows with `FOR UPDATE SKIP LOCKED`, so multiple workers can safely share the queue.
* Redis is used only for atomic per-tenant token buckets, implemented with Lua.
* Batch status is derived from `batch_items` counts instead of being stored on the parent batch row.
* Worker concurrency is bounded with `asyncio.Semaphore`.
* `429` responses respect `Retry-After`; `5xx` and network errors use exponential backoff with jitter and a capped retry count.
* Expired worker leases are swept back to pending so restart recovery does not silently drop work.
* Workers verify lease ownership before calling the vendor and before persisting results, preventing stale workers from overwriting newer state.

## Security And Scope

* Tenant isolation is enforced on every endpoint with `X-Tenant-ID`; real OAuth/JWT auth is intentionally out of scope for this assignment.
* Request bodies with `Content-Length` over 10 MB are rejected, and malformed `Content-Length` values return `400`.
* `.env`, caches, virtual environments, logs, and Git metadata file are excluded from Docker build context with `.dockerignore`.
* Raw idempotency keys are not logged on idempotency conflicts.
* Local Docker credentials in `.env.example` are development defaults only.

## Configuration

All configuration is environment based.

| Variable                         | Default in `.env.example`                                          | Purpose                                                                        |
| -------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------ |
| `DATABASE_URL`                   | `postgresql+asyncpg://postgres:password@postgres:5432/batch_db`   | Runtime PostgreSQL database                                                    |
| `TEST_DATABASE_URL`              | `postgresql+asyncpg://postgres:password@postgres:5432/batch_test` | Isolated pytest database                                                       |
| `REDIS_URL`                      | `redis://redis:6379/0`                                             | Redis connection                                                               |
| `VENDOR_URL`                     | `http://localhost:8080`                                            | Vendor API base URL for local runs                                             |
| `WORKER_POLL_INTERVAL`           | `2`                                                                | Seconds a worker sleeps before polling again when no pending work is available |
| `WORKER_CHUNK_SIZE`              | `10`                                                               | Items claimed per worker poll                                                  |
| `WORKER_LEASE_TIMEOUT`           | `60`                                                               | Seconds before stale in-progress work is recovered                             |
| `MAX_CONCURRENT_REQUESTS`        | `20`                                                               | Max in-flight vendor calls per worker                                          |
| `MAX_RETRY_COUNT`                | `5`                                                                | Attempts before permanent failure                                              |
| `DEFAULT_RATE_LIMIT_CAPACITY`    | `10`                                                               | Default token bucket capacity                                                  |
| `DEFAULT_RATE_LIMIT_REFILL_RATE` | `5`                                                                | Default token refill rate per second                                           |
| `TENANT_RATE_LIMITS`             | `{}`                                                               | Optional JSON tenant overrides                                                 |

Example tenant override:

```env
TENANT_RATE_LIMITS={"tenant-a":{"capacity":10,"refill_rate":5},"tenant-b":{"capacity":3,"refill_rate":1}}
```

## Tests

Tests require PostgreSQL and Redis. The test database is isolated from the runtime database so test teardown cannot drop app tables.

```bash
docker compose up -d postgres redis
docker compose run --rm -e PYTHONDONTWRITEBYTECODE=1 web python -m pytest tests/ -q -p no:cacheprovider
```

The suite covers:

* batch idempotency and tenant isolation
* Redis token bucket capacity, refill, tenant isolation, and concurrent upper-bound behavior
* worker lease recovery, lease ownership validation, stale-worker protection, and tenant-aware claiming
* partial failure status, result reads, and failure manifest reads
* JSON and non-JSON result response serialization
* request body size and malformed `Content-Length` handling

Latest local verification:

```text
24 passed, 2 warnings
```
