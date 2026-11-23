package orchestrator_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/eventsourcing"
	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/orchestrator"
)

type mockWorker struct {
	failStep          string
	executedSteps     []string
	compensatedSteps  []string
	notificationCalls int
}

func (m *mockWorker) ExecuteStep(ctx context.Context, workflowID, stepName, action string, payload json.RawMessage) (json.RawMessage, error) {
	m.executedSteps = append(m.executedSteps, stepName+":"+action)
	if stepName == "Notification" {
		m.notificationCalls++
	}
	if m.failStep == stepName {
		return nil, errors.New("simulated failure in " + stepName)
	}
	return json.RawMessage(`{"status":"ok"}`), nil
}

func (m *mockWorker) CompensateStep(ctx context.Context, workflowID, stepName, compensationAction string, payload json.RawMessage) (json.RawMessage, error) {
	m.compensatedSteps = append(m.compensatedSteps, stepName+":"+compensationAction)
	return json.RawMessage(`{"compensated":true}`), nil
}

func TestEngine_SuccessHappyPath(t *testing.T) {
	ctx := context.Background()
	repo := eventsourcing.NewInMemoryRepository()
	worker := &mockWorker{}
	blueprint := orchestrator.NewCheckoutSagaBlueprint(5 * time.Millisecond)
	engine := orchestrator.NewEngine(repo, blueprint, worker)

	state, err := engine.StartWorkflow(ctx, "wf_happy_1", map[string]interface{}{"amount": 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.Status != eventsourcing.WorkflowStatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s", state.Status)
	}

	expectedExec := []string{
		"Payment:ChargeCard",
		"Inventory:ReserveItems",
		"Fulfillment:GenerateShippingLabel",
		"Notification:SendEmail",
	}
	if len(worker.executedSteps) != len(expectedExec) {
		t.Fatalf("expected %d executed steps, got %d (%v)", len(expectedExec), len(worker.executedSteps), worker.executedSteps)
	}
	for i, exp := range expectedExec {
		if worker.executedSteps[i] != exp {
			t.Fatalf("step %d: expected %s, got %s", i, exp, worker.executedSteps[i])
		}
	}
}

func TestEngine_FulfillmentFailureTriggersReverseCompensations(t *testing.T) {
	ctx := context.Background()
	repo := eventsourcing.NewInMemoryRepository()
	worker := &mockWorker{failStep: "Fulfillment"}
	blueprint := orchestrator.NewCheckoutSagaBlueprint(5 * time.Millisecond)
	engine := orchestrator.NewEngine(repo, blueprint, worker)

	state, err := engine.StartWorkflow(ctx, "wf_rollback_1", map[string]interface{}{"amount": 250})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if state.Status != eventsourcing.WorkflowStatusRolledBack {
		t.Fatalf("expected status ROLLED_BACK, got %s", state.Status)
	}

	// Verify reverse order of compensations: Inventory first, then Payment
	expectedComp := []string{
		"Inventory:ReleaseItems",
		"Payment:RefundCard",
	}
	if len(worker.compensatedSteps) != len(expectedComp) {
		t.Fatalf("expected %d compensations, got %d (%v)", len(expectedComp), len(worker.compensatedSteps), worker.compensatedSteps)
	}
	for i, exp := range expectedComp {
		if worker.compensatedSteps[i] != exp {
			t.Fatalf("compensation %d: expected %s, got %s", i, exp, worker.compensatedSteps[i])
		}
	}
}

func TestEngine_NotificationFailureRetriesAndRoutesToDLQWithoutFailingWorkflow(t *testing.T) {
	ctx := context.Background()
	repo := eventsourcing.NewInMemoryRepository()
	worker := &mockWorker{failStep: "Notification"}
	blueprint := orchestrator.NewCheckoutSagaBlueprint(5 * time.Millisecond)
	engine := orchestrator.NewEngine(repo, blueprint, worker)

	state, err := engine.StartWorkflow(ctx, "wf_dlq_1", map[string]interface{}{"amount": 80})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Workflow must still complete!
	if state.Status != eventsourcing.WorkflowStatusCompleted {
		t.Fatalf("expected status COMPLETED even when Notification fails, got %s", state.Status)
	}

	if worker.notificationCalls != 3 {
		t.Fatalf("expected 3 retry attempts for Notification, got %d", worker.notificationCalls)
	}

	dlqEntries, err := repo.GetDLQEntries(ctx, "wf_dlq_1")
	if err != nil {
		t.Fatalf("unexpected error getting DLQ entries: %v", err)
	}
	if len(dlqEntries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlqEntries))
	}
}
