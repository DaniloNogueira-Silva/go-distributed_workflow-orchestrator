# Master Instruction Document: Distributed Workflow Orchestrator & Edge Rate Limiter

**Context for AI Assistant:**
You are acting as a Senior Staff Engineer. We are going to build a high-performance Distributed Workflow Orchestrator in Go (Golang) that uses the Saga Pattern for distributed transactions and features an Edge Rate Limiter (Token Bucket). 

This system will simulate an E-commerce Checkout Saga: Payment -> Inventory -> Fulfillment -> Notification. 

Please read this entire document to understand the architecture before we start coding. We will build this iteratively, step-by-step.

---

## 1. Architecture & Tech Stack

*   **Language:** Go (Golang)
*   **API/Communication:** REST for the Edge Gateway, gRPC for internal Engine-to-Worker communication.
*   **Database (State & Event Sourcing):** PostgreSQL
*   **Cache & Rate Limiting:** Redis (with Lua scripting for atomicity)
*   **Infrastructure (Local):** Docker Compose (Postgres, Redis, Go Services)

### Architecture Layers:
1.  **Edge API Gateway:** Intercepts external requests, applies inbound rate limiting (Redis Token Bucket), and forwards valid requests to the Orchestrator.
2.  **Orchestrator Engine:** Manages state using Event Sourcing. Records events (`WorkflowStarted`, `PaymentSucceeded`, etc.) in Postgres. Triggers workers. Manages compensations (rollbacks).
3.  **Workers (gRPC):** Execute the actual domain logic. They check an Outbound Rate Limiter before calling "external APIs" (simulated).

---

## 2. The E-commerce Checkout Saga

The Engine must support defining steps with failure and compensation policies.

1.  **Payment Step:** 
    *   Action: Charge Card
    *   On Failure: Fail Workflow
    *   Compensation: Refund Card
2.  **Inventory Step:**
    *   Action: Reserve Items
    *   On Failure: Fail Workflow
    *   Compensation: Release Items
3.  **Fulfillment Step:**
    *   Action: Generate Shipping Label
    *   On Failure: Fail Workflow
    *   Compensation: Void Label
4.  **Notification Step:**
    *   Action: Send Email
    *   On Failure: Retry with Exponential Backoff (Dead Letter Queue if max retries exceeded). *Do not fail the entire workflow.*
    *   Compensation: None (Non-critical step)

---

## 3. Sequential Build Instructions (Prompts)

*Note for User: Copy and paste these prompts one by one to your AI assistant to build the project iteratively.*

### Phase 1: Project Setup and Inbound Rate Limiter
**Prompt 1:**
> "Let's start by setting up the project infrastructure. Create a `docker-compose.yml` with PostgreSQL and Redis. Then, initialize a Go module. Next, create the `Edge API Gateway` in Go. Implement a Redis-based Token Bucket Rate Limiter using a Lua script to ensure atomicity. Create a simple REST endpoint `POST /api/v1/workflows/checkout` that checks the rate limit using an API key from the headers. Return `429 Too Many Requests` if blocked, or `202 Accepted` if allowed. Provide the code and project structure."

### Phase 2: Database Schema & Event Sourcing
**Prompt 2:**
> "Now let's build the database layer for the Orchestrator Engine. We are using Event Sourcing. Write the PostgreSQL schema (migrations) to store the workflows and their event history. We need tables like `workflows` (id, status, created_at) and `workflow_events` (id, workflow_id, event_type, payload, timestamp). Then, write the Go repository layer using `database/sql` or `sqlx` to insert new events and reconstruct the current state of a workflow by replaying its events."

### Phase 3: The Orchestrator Engine Logic (Saga Pattern)
**Prompt 3:**
> "Let's implement the core Saga State Machine. Create the Go logic that defines a Workflow Blueprint (the 4 steps: Payment, Inventory, Fulfillment, Notification). Implement the evaluation function: when a new event is saved, the Engine should evaluate what to do next. If a step fails, the Engine must traverse the history backward and trigger the defined compensation actions for the steps that already succeeded. Keep it abstract enough so we can wire gRPC workers later."

### Phase 4: gRPC Workers & Outbound Rate Limiting
**Prompt 4:**
> "Generate the Protocol Buffers (`.proto` file) defining the communication between the Orchestrator and the Workers. We need RPCs for `ExecuteStep` and `CompensateStep`. Then, implement the 4 mock workers in Go. Before the Payment and Fulfillment workers execute their 'external API' calls, implement an Outbound Rate Limiter checking Redis. If the outbound limit is reached, the worker should not fail; it should delay and retry. Wire these workers to the Orchestrator Engine."

### Phase 5: Tying it all together & Testing
**Prompt 5:**
> "Provide a `main.go` that wires the Edge Gateway, Orchestrator, and gRPC Workers together. Then, write an integration test or a script (using Go or a curl script) that simulates: 1) A successful checkout saga all the way to Notification. 2) A scenario where the Inbound Rate Limiter blocks a client. 3) A scenario where Fulfillment fails, triggering the rollback (compensations) for Inventory and Payment."