# Distributed Workflow Orchestrator & Edge Rate Limiter (Go)

High-performance Distributed Workflow Orchestrator built in Go using **Event Sourcing** (PostgreSQL), the **Saga Pattern** (forward execution & backward compensations), **gRPC Workers**, and **Redis Token Bucket Rate Limiting** (Inbound Edge Gateway & Outbound Worker throttling via atomic Lua scripts).

---

## 🏗️ Architecture Overview

1. **Edge API Gateway (`internal/gateway`):**
   - Intercepts external HTTP requests (`POST /api/v1/workflows/checkout`, `GET /api/v1/workflows/{id}`).
   - Enforces **Inbound Rate Limiting** per `X-API-Key` using an atomic Redis Lua Token Bucket (`429 Too Many Requests` when depleted, `202 Accepted` when allowed).
2. **Orchestrator Engine (`internal/orchestrator` & `internal/eventsourcing`):**
   - Persists immutable domain events (`WorkflowStarted`, `PaymentSucceeded`, `FulfillmentFailed`, `InventoryCompensated`, etc.) in PostgreSQL (`workflows`, `workflow_events`, `dead_letter_queue`).
   - Reconstructs workflow state deterministically by replaying the event history (`ReplayEvents`).
   - Evaluates the **Checkout Saga Blueprint**:
     1. **Payment** (`ChargeCard` / Compensation: `RefundCard`)
     2. **Inventory** (`ReserveItems` / Compensation: `ReleaseItems`)
     3. **Fulfillment** (`GenerateShippingLabel` / Compensation: `VoidLabel`)
     4. **Notification** (`SendEmail` / Non-critical: Retries with Exponential Backoff + Dead Letter Queue without failing the workflow).
3. **gRPC Workers & Outbound Rate Limiter (`internal/worker` & `proto/worker/v1`):**
   - Exposes `WorkerService` (`ExecuteStep` and `CompensateStep` RPCs).
   - Guards external third-party calls in `PaymentWorker` and `FulfillmentWorker` with a Redis **Outbound Rate Limiter** that delays and retries when the external rate limit is reached.

---

## 🚀 Getting Started

### 1. Start Infrastructure (PostgreSQL & Redis)
```bash
docker compose up -d
```

### 2. Run the Full System (Gateway + Orchestrator + gRPC Workers)
```bash
go run main.go
```

### 3. Run All Unit & End-to-End Integration Tests
```bash
go test -v ./...
```

### 4. Run Live Simulation Scripts
- **PowerShell:**
  ```powershell
  ./scripts/simulate_scenarios.ps1
  ```
- **Bash:**
  ```bash
  ./scripts/simulate_scenarios.sh
  ```
