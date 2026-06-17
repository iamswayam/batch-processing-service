CREATE TABLE IF NOT EXISTS batches (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS batch_items (
    id UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    seq INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    payload TEXT NOT NULL,
    result TEXT,
    error_message TEXT,
    attempt_count INT NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    lease_expires_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (batch_id, seq)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    batch_id UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_items_batch_status
    ON batch_items(batch_id, status);

CREATE INDEX IF NOT EXISTS idx_items_claim_queue
    ON batch_items(status, next_retry_at)
    WHERE status IN ('pending', 'in_progress');

CREATE INDEX IF NOT EXISTS idx_items_sweeper
    ON batch_items(lease_expires_at)
    WHERE status = 'in_progress';