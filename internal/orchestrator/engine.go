package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/eventsourcing"
	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/metrics"
)

// WorkerExecutor abstracts the communication with downstream workers (e.g., gRPC Workers).
type WorkerExecutor interface {
	ExecuteStep(ctx context.Context, workflowID, stepName, action string, payload json.RawMessage) (json.RawMessage, error)
	CompensateStep(ctx context.Context, workflowID, stepName, compensationAction string, payload json.RawMessage) (json.RawMessage, error)
}

// Engine manages workflow execution using Event Sourcing and the Saga Pattern.
type Engine struct {
	repo      eventsourcing.Repository
	blueprint *WorkflowBlueprint
	worker    WorkerExecutor
}

// NewEngine creates a new Orchestrator Engine.
func NewEngine(repo eventsourcing.Repository, blueprint *WorkflowBlueprint, worker WorkerExecutor) *Engine {
	if blueprint == nil {
		blueprint = NewCheckoutSagaBlueprint(50 * time.Millisecond)
	}
	return &Engine{
		repo:      repo,
		blueprint: blueprint,
		worker:    worker,
	}
}

func (e *Engine) appendEventWithMetrics(ctx context.Context, workflowID string, eventType eventsourcing.EventType, stepName string, payload interface{}) (*eventsourcing.WorkflowEvent, error) {
	ev, err := e.repo.AppendEvent(ctx, workflowID, eventType, stepName, payload)
	if err == nil {
		metrics.EventsPersistedTotal.WithLabelValues(string(eventType)).Inc()
	}
	return ev, err
}

// StartWorkflow creates a new workflow event stream and evaluates the Saga state machine until completion or rollback.
func (e *Engine) StartWorkflow(ctx context.Context, workflowID string, initialPayload interface{}) (*eventsourcing.WorkflowState, error) {
	startTime := time.Now()
	metrics.WorkflowsInFlight.Inc()
	defer metrics.WorkflowsInFlight.Dec()

	_, err := e.repo.CreateWorkflow(ctx, workflowID, e.blueprint.Name, initialPayload)
	if err != nil {
		return nil, fmt.Errorf("create workflow %s: %w", workflowID, err)
	}
	metrics.EventsPersistedTotal.WithLabelValues(string(eventsourcing.EventWorkflowStarted)).Inc()

	log.Printf("[Orchestrator] Workflow %s started (%s)", workflowID, e.blueprint.Name)
	state, evalErr := e.Evaluate(ctx, workflowID)
	if state != nil {
		metrics.WorkflowsTotal.WithLabelValues(string(state.Status)).Inc()
		metrics.WorkflowDuration.WithLabelValues(string(state.Status)).Observe(time.Since(startTime).Seconds())
	} else if evalErr != nil {
		metrics.WorkflowsTotal.WithLabelValues("ERROR").Inc()
	}
	return state, evalErr
}

// GetWorkflowState reconstructs and returns the current state of a workflow by replaying its events.
func (e *Engine) GetWorkflowState(ctx context.Context, workflowID string) (*eventsourcing.WorkflowState, error) {
	return e.repo.ReconstructState(ctx, workflowID)
}

// Evaluate replays the workflow's event history, determines what action to take next, and advances the Saga state machine.
func (e *Engine) Evaluate(ctx context.Context, workflowID string) (*eventsourcing.WorkflowState, error) {
	for {
		state, err := e.repo.ReconstructState(ctx, workflowID)
		if err != nil {
			return nil, fmt.Errorf("reconstruct state for %s: %w", workflowID, err)
		}

		// Terminal states: nothing more to evaluate
		if state.Status == eventsourcing.WorkflowStatusCompleted ||
			state.Status == eventsourcing.WorkflowStatusRolledBack ||
			state.Status == eventsourcing.WorkflowStatusFailed {
			return state, nil
		}

		// If the workflow is in COMPENSATING status, execute backward compensations
		if state.Status == eventsourcing.WorkflowStatusCompensating {
			if err := e.runCompensationsBackward(ctx, state); err != nil {
				return nil, err
			}
			continue
		}

		// Find the next step in the blueprint that hasn't succeeded or been dead-lettered
		nextStep, found := e.findNextStep(state)
		if !found {
			// All steps in the blueprint have completed (or non-critical step was dead-lettered)
			_, err := e.appendEventWithMetrics(ctx, workflowID, eventsourcing.EventWorkflowCompleted, "", map[string]string{
				"status": "COMPLETED",
			})
			if err != nil {
				return nil, fmt.Errorf("append WorkflowCompleted: %w", err)
			}
			log.Printf("[Orchestrator] Workflow %s COMPLETED successfully", workflowID)
			return e.repo.ReconstructState(ctx, workflowID)
		}

		// Execute the next step according to its FailurePolicy
		if err := e.executeStepWithPolicy(ctx, state, nextStep); err != nil {
			return nil, err
		}
	}
}

func (e *Engine) findNextStep(state *eventsourcing.WorkflowState) (StepDefinition, bool) {
	for _, stepDef := range e.blueprint.Steps {
		st, exists := state.StepStates[stepDef.Name]
		if !exists {
			return stepDef, true
		}
		if st.Status != eventsourcing.StepStatusSucceeded && st.Status != eventsourcing.StepStatusDeadLettered {
			return stepDef, true
		}
	}
	return StepDefinition{}, false
}

func (e *Engine) executeStepWithPolicy(ctx context.Context, state *eventsourcing.WorkflowState, step StepDefinition) error {
	workflowID := state.WorkflowID
	maxAttempts := step.MaxRetries
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if _, err := e.appendEventWithMetrics(ctx, workflowID, step.StartEvent, step.Name, eventsourcing.StepEventPayload{
			StepName: step.Name,
			Attempt:  attempt,
		}); err != nil {
			return fmt.Errorf("append start event for %s: %w", step.Name, err)
		}

		log.Printf("[Orchestrator] Workflow %s -> Executing step '%s' (Action: %s, Attempt: %d/%d)",
			workflowID, step.Name, step.Action, attempt, maxAttempts)

		stepStart := time.Now()
		output, execErr := e.worker.ExecuteStep(ctx, workflowID, step.Name, step.Action, state.InitialPayload)
		metrics.SagaStepDuration.WithLabelValues(step.Name, "execute").Observe(time.Since(stepStart).Seconds())

		if execErr == nil {
			metrics.SagaStepExecutionsTotal.WithLabelValues(step.Name, step.Action, "SUCCEEDED").Inc()
			_, err := e.appendEventWithMetrics(ctx, workflowID, step.SuccessEvent, step.Name, eventsourcing.StepEventPayload{
				StepName: step.Name,
				Attempt:  attempt,
				Output:   output,
			})
			if err != nil {
				return fmt.Errorf("append success event for %s: %w", step.Name, err)
			}
			log.Printf("[Orchestrator] Workflow %s -> Step '%s' SUCCEEDED", workflowID, step.Name)
			return nil
		}

		lastErr = execErr
		metrics.SagaStepExecutionsTotal.WithLabelValues(step.Name, step.Action, "FAILED").Inc()
		log.Printf("[Orchestrator] Workflow %s -> Step '%s' FAILED on attempt %d/%d: %v",
			workflowID, step.Name, attempt, maxAttempts, execErr)

		// If RetryThenDLQ and more attempts remain, wait with exponential backoff
		if step.FailurePolicy == PolicyRetryThenDLQ && attempt < maxAttempts {
			backoff := time.Duration(float64(step.InitialBackoff) * math.Pow(2, float64(attempt-1)))
			_, _ = e.appendEventWithMetrics(ctx, workflowID, eventsourcing.EventStepRetryScheduled, step.Name, eventsourcing.StepEventPayload{
				StepName: step.Name,
				Attempt:  attempt,
				Error:    execErr.Error(),
			})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	// All attempts for this step failed
	_, err := e.appendEventWithMetrics(ctx, workflowID, step.FailEvent, step.Name, eventsourcing.StepEventPayload{
		StepName: step.Name,
		Attempt:  maxAttempts,
		Error:    lastErr.Error(),
	})
	if err != nil {
		return fmt.Errorf("append fail event for %s: %w", step.Name, err)
	}

	switch step.FailurePolicy {
	case PolicyRetryThenDLQ:
		// Non-critical step (e.g., Notification): send to DLQ and DO NOT fail the entire workflow
		dlqEvent := step.DLQEvent
		if dlqEvent == "" {
			dlqEvent = eventsourcing.EventStepDeadLettered
		}
		if _, err := e.appendEventWithMetrics(ctx, workflowID, dlqEvent, step.Name, eventsourcing.StepEventPayload{
			StepName: step.Name,
			Attempt:  maxAttempts,
			Error:    fmt.Sprintf("max retries (%d) exceeded: %v", maxAttempts, lastErr),
		}); err != nil {
			return fmt.Errorf("append DLQ event: %w", err)
		}

		if err := e.repo.SaveToDLQ(ctx, workflowID, step.Name, lastErr.Error(), state.InitialPayload); err != nil {
			return fmt.Errorf("save to DLQ: %w", err)
		}

		metrics.DLQMessagesTotal.WithLabelValues(step.Name).Inc()
		metrics.SagaStepExecutionsTotal.WithLabelValues(step.Name, step.Action, "DEAD_LETTERED").Inc()
		log.Printf("[Orchestrator] Workflow %s -> Step '%s' exhausted %d retries; sent to Dead Letter Queue (workflow continues)",
			workflowID, step.Name, maxAttempts)
		return nil

	default: // PolicyFailAndCompensate
		log.Printf("[Orchestrator] Workflow %s -> Step '%s' failed critically; initiating Saga rollback (compensations)",
			workflowID, step.Name)
		_, err := e.appendEventWithMetrics(ctx, workflowID, eventsourcing.EventWorkflowCompensating, "", map[string]string{
			"failed_step": step.Name,
			"reason":      lastErr.Error(),
		})
		return err
	}
}

// runCompensationsBackward traverses the completed steps in reverse order and executes their compensations.
func (e *Engine) runCompensationsBackward(ctx context.Context, state *eventsourcing.WorkflowState) error {
	workflowID := state.WorkflowID

	// Traverse CompletedStepsOrder backward (LIFO)
	for i := len(state.CompletedStepsOrder) - 1; i >= 0; i-- {
		stepName := state.CompletedStepsOrder[i]
		stepState, exists := state.StepStates[stepName]
		if !exists || stepState.Compensated {
			continue
		}

		stepDef, found := e.blueprint.StepByName(stepName)
		if !found || !stepDef.HasCompensation {
			continue
		}

		log.Printf("[Orchestrator] Workflow %s -> Compensating step '%s' (Action: %s)",
			workflowID, stepDef.Name, stepDef.CompensationAction)

		if _, err := e.appendEventWithMetrics(ctx, workflowID, eventsourcing.EventStepCompensationStart, stepDef.Name, eventsourcing.StepEventPayload{
			StepName: stepDef.Name,
		}); err != nil {
			return fmt.Errorf("append compensation start for %s: %w", stepDef.Name, err)
		}

		compStart := time.Now()
		compOutput, err := e.worker.CompensateStep(ctx, workflowID, stepDef.Name, stepDef.CompensationAction, state.InitialPayload)
		metrics.SagaStepDuration.WithLabelValues(stepDef.Name, "compensate").Observe(time.Since(compStart).Seconds())

		if err != nil {
			// Record critical failure if compensation itself fails
			_, _ = e.appendEventWithMetrics(ctx, workflowID, eventsourcing.EventWorkflowFailed, stepDef.Name, map[string]string{
				"error": fmt.Sprintf("compensation failed for %s: %v", stepDef.Name, err),
			})
			return fmt.Errorf("compensate step %s: %w", stepDef.Name, err)
		}

		metrics.SagaStepExecutionsTotal.WithLabelValues(stepDef.Name, stepDef.CompensationAction, "COMPENSATED").Inc()

		compEvent := stepDef.CompensateEvent
		if compEvent == "" {
			compEvent = eventsourcing.EventStepCompensated
		}

		if _, err := e.appendEventWithMetrics(ctx, workflowID, compEvent, stepDef.Name, eventsourcing.StepEventPayload{
			StepName: stepDef.Name,
			Output:   compOutput,
		}); err != nil {
			return fmt.Errorf("append compensated event for %s: %w", stepDef.Name, err)
		}

		log.Printf("[Orchestrator] Workflow %s -> Step '%s' COMPENSATED (%s)",
			workflowID, stepDef.Name, stepDef.CompensationAction)
	}

	_, err := e.appendEventWithMetrics(ctx, workflowID, eventsourcing.EventWorkflowRolledBack, "", map[string]string{
		"status": "ROLLED_BACK",
	})
	if err != nil {
		return fmt.Errorf("append WorkflowRolledBack: %w", err)
	}

	log.Printf("[Orchestrator] Workflow %s ROLLED_BACK completely", workflowID)
	return nil
}
