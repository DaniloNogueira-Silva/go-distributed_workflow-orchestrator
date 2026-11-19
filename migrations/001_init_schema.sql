-- 001_init_schema.sql
-- Schema for Distributed Workflow Orchestrator (Event Sourcing, Saga Pattern & Domain Worker Tables)

-- ============================================================================
-- 1. ORCHESTRATOR EVENT SOURCING & DLQ TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS workflows (
    id VARCHAR(64) PRIMARY KEY,
    workflow_type VARCHAR(64) NOT NULL DEFAULT 'CHECKOUT_SAGA',
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_events (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    step_name VARCHAR(64) NOT NULL DEFAULT '',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_workflow_id
    ON workflow_events(workflow_id, id ASC);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id BIGSERIAL PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_name VARCHAR(64) NOT NULL,
    reason TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_workflow_id
    ON dead_letter_queue(workflow_id);

-- ============================================================================
-- 2. PAYMENT WORKER DOMAIN TABLES (ACCOUNTS & TRANSACTION LEDGER)
-- ============================================================================

CREATE TABLE IF NOT EXISTS payment_accounts (
    customer_id VARCHAR(64) PRIMARY KEY,
    balance NUMERIC(14, 2) NOT NULL DEFAULT 25000.00,
    currency VARCHAR(8) NOT NULL DEFAULT 'BRL',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_transactions (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL UNIQUE,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(14, 2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    status VARCHAR(32) NOT NULL, -- 'CHARGED' | 'REFUNDED'
    refund_id VARCHAR(64) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_tx_workflow_id
    ON payment_transactions(workflow_id);

-- ============================================================================
-- 3. INVENTORY WORKER DOMAIN TABLES (CATALOG STOCK & RESERVATIONS)
-- ============================================================================

CREATE TABLE IF NOT EXISTS inventory_products (
    sku VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    available_stock INT NOT NULL CHECK (available_stock >= 0),
    reserved_stock INT NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL UNIQUE,
    items JSONB NOT NULL DEFAULT '[]'::jsonb,
    status VARCHAR(32) NOT NULL, -- 'RESERVED' | 'RELEASED'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inventory_res_workflow_id
    ON inventory_reservations(workflow_id);

-- ============================================================================
-- 4. FULFILLMENT WORKER DOMAIN TABLE (SHIPPING LABELS)
-- ============================================================================

CREATE TABLE IF NOT EXISTS fulfillment_shipments (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL UNIQUE,
    customer_id VARCHAR(64) NOT NULL,
    tracking_code VARCHAR(64) NOT NULL,
    carrier VARCHAR(64) NOT NULL DEFAULT 'EXPRESS_LOGISTICS',
    item_count INT NOT NULL,
    status VARCHAR(32) NOT NULL, -- 'LABEL_GENERATED' | 'VOIDED'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 5. NOTIFICATION WORKER DOMAIN TABLE (TRANSACTIONAL OUTBOX)
-- ============================================================================

CREATE TABLE IF NOT EXISTS notification_messages (
    id VARCHAR(64) PRIMARY KEY,
    workflow_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    recipient_email VARCHAR(128) NOT NULL,
    subject VARCHAR(255) NOT NULL,
    body TEXT NOT NULL,
    status VARCHAR(32) NOT NULL, -- 'SENT'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
