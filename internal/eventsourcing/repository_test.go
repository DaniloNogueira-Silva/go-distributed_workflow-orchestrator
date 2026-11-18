package eventsourcing_test

import (
	"context"
	"testing"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/eventsourcing"
)

func TestReplayEvents_SuccessAndRollbackReconstruction(t *testing.T) {
	ctx := context.Background()
	repo := eventsourcing.NewInMemoryRepository()

	wfID := "wf_test_1"
	_, err := repo.CreateWorkflow(ctx, wfID, "CHECKOUT_SAGA", map[string]interface{}{
		"customer_id": "cust_99",
		"amount":      199.90,
	})
	if err != nil {
		t.Fatalf("failed to create workflow: %v", err)
	}

	// Simulate Payment and Inventory succeeding, then Fulfillment failing
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventPaymentStarted, "Payment", nil)
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventPaymentSucceeded, "Payment", map[string]string{"tx_id": "pay_1"})

	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventInventoryStarted, "Inventory", nil)
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventInventorySucceeded, "Inventory", map[string]string{"res_id": "inv_1"})

	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventFulfillmentStarted, "Fulfillment", nil)
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventFulfillmentFailed, "Fulfillment", eventsourcing.StepEventPayload{
		StepName: "Fulfillment",
		Error:    "carrier unavailable",
	})

	state, err := repo.ReconstructState(ctx, wfID)
	if err != nil {
		t.Fatalf("failed to reconstruct state: %v", err)
	}

	if len(state.CompletedStepsOrder) != 2 {
		t.Fatalf("expected 2 completed steps, got %d", len(state.CompletedStepsOrder))
	}
	if state.CompletedStepsOrder[0] != "Payment" || state.CompletedStepsOrder[1] != "Inventory" {
		t.Fatalf("unexpected completed steps order: %v", state.CompletedStepsOrder)
	}
	if state.StepStates["Fulfillment"].Status != eventsourcing.StepStatusFailed {
		t.Fatalf("expected Fulfillment status FAILED, got %s", state.StepStates["Fulfillment"].Status)
	}

	// Simulate Compensations in reverse order: Inventory -> Payment
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventWorkflowCompensating, "", nil)
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventInventoryCompensated, "Inventory", nil)
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventPaymentCompensated, "Payment", nil)
	_, _ = repo.AppendEvent(ctx, wfID, eventsourcing.EventWorkflowRolledBack, "", nil)

	finalState, err := repo.ReconstructState(ctx, wfID)
	if err != nil {
		t.Fatalf("failed to reconstruct final state: %v", err)
	}

	if finalState.Status != eventsourcing.WorkflowStatusRolledBack {
		t.Fatalf("expected workflow status ROLLED_BACK, got %s", finalState.Status)
	}
	if !finalState.StepStates["Inventory"].Compensated || !finalState.StepStates["Payment"].Compensated {
		t.Fatalf("expected both Inventory and Payment to be marked compensated")
	}
}
