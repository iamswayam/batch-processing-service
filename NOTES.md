# NOTES

Engineering decisions, tradeoffs, follow-up work, and reflections from building this assignment.

---

# Time Spent

Approximately **8–9 hours**.

Most of the time was spent on architecture decisions, validating correctness around idempotency, durable processing, retries, failure handling, and performing extensive manual verification beyond the automated test suite.

---

# AI Tools Used

AI tools (ChatGPT, Claude, and Codex) were used throughout the assignment as development assistants.

Specifically, they were used for:

* Architecture brainstorming and design tradeoff discussions.
* Exploring alternative implementation approaches.
* Generating initial project scaffolding and boilerplate.
* Assisting with documentation and test development.
* Debugging implementation issues and validating edge cases during development.

All generated suggestions and code were manually reviewed, validated, tested, and adapted before being incorporated into the final solution. I understand the implementation and can explain the design decisions and tradeoffs made throughout the project.

---

# Reused Code / Boilerplate

No application logic or personal project boilerplate was reused for this assignment.

Only general development conventions and project structure patterns influenced the initial setup. The business logic, persistence model, rate limiting, worker coordination, retry handling, and idempotency implementation were developed specifically for this assignment.

---

# Key Design Decisions

## PostgreSQL as the durable queue

Workers claim items directly from PostgreSQL using `SELECT ... FOR UPDATE SKIP LOCKED`. This provides durable state, safe concurrent processing, and restart recovery without introducing a separate queueing system.

## Redis only for distributed rate limiting

Redis is used exclusively for per-tenant token bucket rate limiting through an atomic Lua script. Job state and processing remain in PostgreSQL, keeping responsibilities clearly separated.

## Aggregate-on-read batch status

Batch status is derived from item counts instead of being continuously updated. This avoids unnecessary write contention when multiple workers finish items concurrently.

## Tenant rate limit configuration

Rate limits are enforced per tenant with configurable capacity and refill values. A database-backed configuration model was intentionally avoided to keep the implementation aligned with the assignment scope.

## Idempotency approach

Batch submission is strictly idempotent, while worker processing relies on durable state, lease-based recovery, lease ownership validation, and conditional state transitions to safely coordinate concurrent workers and prevent stale workers from overwriting newer state. Exactly-once external side effects ultimately depend on vendor-supported idempotency mechanisms, which are outside the scope of this assignment.

---

# What I Cut

To keep the solution focused and maintainable, I intentionally did not implement:

* Celery, RabbitMQ, Kafka, or additional broker infrastructure.
* A database-backed tenant administration model.
* Pagination, cancellation APIs, webhooks, or admin dashboards.
* OAuth/JWT authentication beyond tenant isolation.
* Production infrastructure concerns such as secret management or TLS termination.

I preferred solving the core correctness requirements well instead of adding more infrastructure.

---

# What I Would Do Differently With More Time

The implementation intentionally focuses on correctness and the assignment's core requirements. With additional time, I would prioritize operational improvements rather than architectural changes.

* Add pagination for large result sets to improve scalability and client experience for very large batches.
* Improve observability by exposing metrics such as queue depth, processing throughput, retry counts, and permanent failure rates.
* Expand integration and stress testing to further validate restart recovery, rate limiting, and idempotency under sustained multi-worker load.
* Add graceful worker shutdown so in-flight requests can complete before termination and abandoned work can be reclaimed predictably.
* Support batch cancellation for long-running jobs while preserving consistency for already completed work.

---

# Additional Manual Verification

Beyond the automated test suite, I manually exercised the service to validate runtime behavior and important edge cases.

* Verified strict batch idempotency (same `Idempotency-Key` and same payload returns the original `batch_id` without creating new work).
* Verified `409 Conflict` when the same idempotency key is reused with a different payload.
* Verified tenant isolation across all endpoints.
* Verified asynchronous processing with both small and larger batches (including 100 and 500 items), progressive status updates, retry behavior for `500` and `429` responses, worker restart recovery, and duplicate submission handling using the mock vendor's admin endpoints during development.
* Verified partial failure behavior together with the `/results` and `/failures` endpoints while processing was still in progress.
* Verified lease ownership validation prevents stale workers from overwriting newer state after lease expiry, supported by targeted regression testing.

---

# Refinements Made During Manual Testing

Manual API testing identified two response-layer improvements that were implemented before submission.

* Normalized the `/batches/{batch_id}/results` response to return parsed JSON objects when the stored value contains valid JSON, while preserving the original string for non-JSON values for backward compatibility.
* Updated the response model to use a JSON-compatible type so the API contract accurately reflects the returned payload.

---

# Challenges

The primary challenge was validating nondeterministic failure scenarios.

The provided mock vendor intentionally introduces random latency, `500` responses, and `429` rate limits, making certain execution paths difficult to reproduce consistently. To increase confidence, I supplemented the automated test suite with extensive end-to-end manual verification covering idempotency, retries, restart recovery, partial failures, tenant isolation, and lease ownership validation under concurrent worker execution.

---

# Known Limitations

* External exactly-once processing ultimately depends on vendor-supported idempotency mechanisms.
* Result and failure endpoints currently do not paginate very large batches.
* Authentication is intentionally limited to tenant isolation via `X-Tenant-ID`, consistent with the assignment scope.
* Observability is currently limited to logs and health endpoints; production deployments would benefit from richer metrics and dashboards.

---

# What I Am Proud Of

* Keeping the architecture simple while satisfying the core correctness requirements.
* Using PostgreSQL as the durable source of truth and Redis only where distributed coordination is required.
* Achieving safe concurrent processing without introducing unnecessary queue infrastructure.
* Strengthening worker lease ownership semantics so stale workers cannot overwrite state after lease recovery while preserving a simple architecture.
* Treating partial failures as first-class citizens while allowing successful results to remain accessible throughout processing.
* Focusing testing effort on the highest-risk correctness paths instead of chasing superficial coverage numbers.
* Keeping the implementation aligned with the assignment scope by prioritizing correctness and simplicity over additional infrastructure or features.

---

# Reflection

My primary goal throughout this assignment was **correctness over complexity**.

Whenever multiple implementation options existed, I preferred the simplest design that could reliably satisfy the requirements for durability, idempotency, partial failure handling, distributed rate limiting, and restart recovery. I believe this results in a system that is easier to understand, test, maintain, and evolve.
