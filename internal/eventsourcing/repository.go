package eventsourcing

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// DLQEntry represents a Dead Letter Queue item for failed non-critical steps.
type DLQEntry struct {
	ID         int64           `json:"id"`
	WorkflowID string          `json:"workflow_id"`
	StepName   string          `json:"step_name"`
	Reason     string          `json:"reason"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
}

// Repository defines the Event Sourcing storage interface.
type Repository interface {
	CreateWorkflow(ctx context.Context, workflowID, workflowType string, initialPayload interface{}) (*WorkflowEvent, error)
	AppendEvent(ctx context.Context, workflowID string, eventType EventType, stepName string, payload interface{}) (*WorkflowEvent, error)
	UpdateWorkflowStatus(ctx context.Context, workflowID string, status WorkflowStatus) error
	GetEvents(ctx context.Context, workflowID string) ([]WorkflowEvent, error)
	ReconstructState(ctx context.Context, workflowID string) (*WorkflowState, error)
	SaveToDLQ(ctx context.Context, workflowID, stepName, reason string, payload interface{}) error
	GetDLQEntries(ctx context.Context, workflowID string) ([]DLQEntry, error)
}

// PostgresRepository implements Repository backed by PostgreSQL.
type PostgresRepository struct {
	db *sql.DB
}

// NewPostgresRepository creates a new PostgresRepository instance.
func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

// EnsureSchema runs the initial DDL schema to create tables if they do not exist.
func (r *PostgresRepository) EnsureSchema(ctx context.Context) error {
	schema := `
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
	`
	_, err := r.db.ExecContext(ctx, schema)
	return err
}

// CreateWorkflow initializes the workflow record and inserts the first WorkflowStarted event atomically.
func (r *PostgresRepository) CreateWorkflow(ctx context.Context, workflowID, workflowType string, initialPayload interface{}) (*WorkflowEvent, error) {
	payloadBytes, err := marshalPayload(initialPayload)
	if err != nil {
		return nil, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO workflows (id, workflow_type, status, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
	`, workflowID, workflowType, string(WorkflowStatusRunning))
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}

	var ev WorkflowEvent
	var payloadRaw []byte
	err = tx.QueryRowContext(ctx, `
		INSERT INTO workflow_events (workflow_id, event_type, step_name, payload, timestamp)
		VALUES ($1, $2, '', $3, NOW())
		RETURNING id, workflow_id, event_type, step_name, payload, timestamp
	`, workflowID, string(EventWorkflowStarted), payloadBytes).Scan(
		&ev.ID, &ev.WorkflowID, &ev.EventType, &ev.StepName, &payloadRaw, &ev.Timestamp,
	)
	if err != nil {
		return nil, fmt.Errorf("insert initial event: %w", err)
	}
	ev.Payload = json.RawMessage(payloadRaw)

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &ev, nil
}

// AppendEvent inserts an immutable event into workflow_events and optionally updates workflow status.
func (r *PostgresRepository) AppendEvent(ctx context.Context, workflowID string, eventType EventType, stepName string, payload interface{}) (*WorkflowEvent, error) {
	payloadBytes, err := marshalPayload(payload)
	if err != nil {
		return nil, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var ev WorkflowEvent
	var payloadRaw []byte
	err = tx.QueryRowContext(ctx, `
		INSERT INTO workflow_events (workflow_id, event_type, step_name, payload, timestamp)
		VALUES ($1, $2, $3, $4, NOW())
		RETURNING id, workflow_id, event_type, step_name, payload, timestamp
	`, workflowID, string(eventType), stepName, payloadBytes).Scan(
		&ev.ID, &ev.WorkflowID, &ev.EventType, &ev.StepName, &payloadRaw, &ev.Timestamp,
	)
	if err != nil {
		return nil, fmt.Errorf("append event: %w", err)
	}
	ev.Payload = json.RawMessage(payloadRaw)

	// Automatically synchronize high-level workflow status if terminal/transition event
	if newStatus, ok := statusFromEvent(eventType); ok {
		_, err = tx.ExecContext(ctx, `
			UPDATE workflows SET status = $1, updated_at = NOW() WHERE id = $2
		`, string(newStatus), workflowID)
		if err != nil {
			return nil, fmt.Errorf("update workflow status: %w", err)
		}
	} else {
		_, _ = tx.ExecContext(ctx, `UPDATE workflows SET updated_at = NOW() WHERE id = $1`, workflowID)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return &ev, nil
}

// UpdateWorkflowStatus explicitly updates the status column of a workflow.
func (r *PostgresRepository) UpdateWorkflowStatus(ctx context.Context, workflowID string, status WorkflowStatus) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE workflows SET status = $1, updated_at = NOW() WHERE id = $2
	`, string(status), workflowID)
	return err
}

// GetEvents retrieves all events for a workflow ordered chronologically by ID.
func (r *PostgresRepository) GetEvents(ctx context.Context, workflowID string) ([]WorkflowEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, workflow_id, event_type, step_name, payload, timestamp
		FROM workflow_events
		WHERE workflow_id = $1
		ORDER BY id ASC
	`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []WorkflowEvent
	for rows.Next() {
		var ev WorkflowEvent
		var payloadRaw []byte
		if err := rows.Scan(&ev.ID, &ev.WorkflowID, &ev.EventType, &ev.StepName, &payloadRaw, &ev.Timestamp); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		ev.Payload = json.RawMessage(payloadRaw)
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// ReconstructState fetches all events for a workflow and replays them to build the current state.
func (r *PostgresRepository) ReconstructState(ctx context.Context, workflowID string) (*WorkflowState, error) {
	events, err := r.GetEvents(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return ReplayEvents(workflowID, events)
}

// SaveToDLQ records a failed non-critical step in the Dead Letter Queue.
func (r *PostgresRepository) SaveToDLQ(ctx context.Context, workflowID, stepName, reason string, payload interface{}) error {
	payloadBytes, err := marshalPayload(payload)
	if err != nil {
		return err
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO dead_letter_queue (workflow_id, step_name, reason, payload, created_at)
		VALUES ($1, $2, $3, $4, NOW())
	`, workflowID, stepName, reason, payloadBytes)
	return err
}

// GetDLQEntries fetches DLQ records for a workflow.
func (r *PostgresRepository) GetDLQEntries(ctx context.Context, workflowID string) ([]DLQEntry, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, workflow_id, step_name, reason, payload, created_at
		FROM dead_letter_queue
		WHERE workflow_id = $1
		ORDER BY id ASC
	`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []DLQEntry
	for rows.Next() {
		var e DLQEntry
		var raw []byte
		if err := rows.Scan(&e.ID, &e.WorkflowID, &e.StepName, &e.Reason, &raw, &e.CreatedAt); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(raw)
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// InMemoryRepository provides a thread-safe in-memory implementation of Repository for testing and standalone execution.
type InMemoryRepository struct {
	mu        sync.RWMutex
	seq       int64
	workflows map[string]WorkflowStatus
	events    map[string][]WorkflowEvent
	dlq       map[string][]DLQEntry
}

// NewInMemoryRepository creates a new InMemoryRepository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		workflows: make(map[string]WorkflowStatus),
		events:    make(map[string][]WorkflowEvent),
		dlq:       make(map[string][]DLQEntry),
	}
}

func (m *InMemoryRepository) CreateWorkflow(ctx context.Context, workflowID, workflowType string, initialPayload interface{}) (*WorkflowEvent, error) {
	payloadBytes, err := marshalPayload(initialPayload)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.workflows[workflowID]; exists {
		return nil, fmt.Errorf("workflow %s already exists", workflowID)
	}

	m.seq++
	ev := WorkflowEvent{
		ID:         m.seq,
		WorkflowID: workflowID,
		EventType:  EventWorkflowStarted,
		StepName:   "",
		Payload:    json.RawMessage(payloadBytes),
		Timestamp:  time.Now().UTC(),
	}

	m.workflows[workflowID] = WorkflowStatusRunning
	m.events[workflowID] = []WorkflowEvent{ev}
	return &ev, nil
}

func (m *InMemoryRepository) AppendEvent(ctx context.Context, workflowID string, eventType EventType, stepName string, payload interface{}) (*WorkflowEvent, error) {
	payloadBytes, err := marshalPayload(payload)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.workflows[workflowID]; !exists {
		return nil, fmt.Errorf("workflow %s not found", workflowID)
	}

	m.seq++
	ev := WorkflowEvent{
		ID:         m.seq,
		WorkflowID: workflowID,
		EventType:  eventType,
		StepName:   stepName,
		Payload:    json.RawMessage(payloadBytes),
		Timestamp:  time.Now().UTC(),
	}

	m.events[workflowID] = append(m.events[workflowID], ev)

	if newStatus, ok := statusFromEvent(eventType); ok {
		m.workflows[workflowID] = newStatus
	}

	return &ev, nil
}

func (m *InMemoryRepository) UpdateWorkflowStatus(ctx context.Context, workflowID string, status WorkflowStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workflows[workflowID] = status
	return nil
}

func (m *InMemoryRepository) GetEvents(ctx context.Context, workflowID string) ([]WorkflowEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	evs, exists := m.events[workflowID]
	if !exists {
		return nil, fmt.Errorf("workflow %s not found", workflowID)
	}

	copied := make([]WorkflowEvent, len(evs))
	copy(copied, evs)
	return copied, nil
}

func (m *InMemoryRepository) ReconstructState(ctx context.Context, workflowID string) (*WorkflowState, error) {
	evs, err := m.GetEvents(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return ReplayEvents(workflowID, evs)
}

func (m *InMemoryRepository) SaveToDLQ(ctx context.Context, workflowID, stepName, reason string, payload interface{}) error {
	payloadBytes, err := marshalPayload(payload)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	entry := DLQEntry{
		ID:         m.seq,
		WorkflowID: workflowID,
		StepName:   stepName,
		Reason:     reason,
		Payload:    json.RawMessage(payloadBytes),
		CreatedAt:  time.Now().UTC(),
	}
	m.dlq[workflowID] = append(m.dlq[workflowID], entry)
	return nil
}

func (m *InMemoryRepository) GetDLQEntries(ctx context.Context, workflowID string) ([]DLQEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entries := m.dlq[workflowID]
	copied := make([]DLQEntry, len(entries))
	copy(copied, entries)
	return copied, nil
}

func marshalPayload(payload interface{}) ([]byte, error) {
	if payload == nil {
		return []byte("{}"), nil
	}
	switch v := payload.(type) {
	case []byte:
		if len(v) == 0 {
			return []byte("{}"), nil
		}
		return v, nil
	case json.RawMessage:
		if len(v) == 0 {
			return []byte("{}"), nil
		}
		return v, nil
	default:
		return json.Marshal(payload)
	}
}

func statusFromEvent(eventType EventType) (WorkflowStatus, bool) {
	switch eventType {
	case EventWorkflowStarted:
		return WorkflowStatusRunning, true
	case EventWorkflowCompensating:
		return WorkflowStatusCompensating, true
	case EventWorkflowRolledBack:
		return WorkflowStatusRolledBack, true
	case EventWorkflowCompleted:
		return WorkflowStatusCompleted, true
	case EventWorkflowFailed:
		return WorkflowStatusFailed, true
	default:
		return "", false
	}
}
