package eventsourcing

import (
	"encoding/json"
	"fmt"
	"time"
)

// WorkflowStatus represents the high-level lifecycle state of a workflow.
type WorkflowStatus string

const (
	WorkflowStatusPending      WorkflowStatus = "PENDING"
	WorkflowStatusRunning      WorkflowStatus = "RUNNING"
	WorkflowStatusCompensating WorkflowStatus = "COMPENSATING"
	WorkflowStatusRolledBack   WorkflowStatus = "ROLLED_BACK"
	WorkflowStatusCompleted    WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed       WorkflowStatus = "FAILED"
)

// StepStatus represents the status of an individual saga step.
type StepStatus string

const (
	StepStatusPending      StepStatus = "PENDING"
	StepStatusInProgress   StepStatus = "IN_PROGRESS"
	StepStatusSucceeded    StepStatus = "SUCCEEDED"
	StepStatusFailed       StepStatus = "FAILED"
	StepStatusCompensating StepStatus = "COMPENSATING"
	StepStatusCompensated  StepStatus = "COMPENSATED"
	StepStatusDeadLettered StepStatus = "DEAD_LETTERED"
)

// EventType enumerates all domain events stored in the event log.
type EventType string

const (
	EventWorkflowStarted      EventType = "WorkflowStarted"
	EventWorkflowCompensating EventType = "WorkflowCompensating"
	EventWorkflowRolledBack   EventType = "WorkflowRolledBack"
	EventWorkflowCompleted    EventType = "WorkflowCompleted"
	EventWorkflowFailed       EventType = "WorkflowFailed"

	// Generic step lifecycle events
	EventStepStarted           EventType = "StepStarted"
	EventStepSucceeded         EventType = "StepSucceeded"
	EventStepFailed            EventType = "StepFailed"
	EventStepRetryScheduled    EventType = "StepRetryScheduled"
	EventStepDeadLettered      EventType = "StepDeadLettered"
	EventStepCompensationStart EventType = "StepCompensationStarted"
	EventStepCompensated       EventType = "StepCompensated"

	// Named domain events for E-commerce Checkout Saga
	EventPaymentStarted     EventType = "PaymentStarted"
	EventPaymentSucceeded   EventType = "PaymentSucceeded"
	EventPaymentFailed      EventType = "PaymentFailed"
	EventPaymentCompensated EventType = "PaymentCompensated"

	EventInventoryStarted     EventType = "InventoryStarted"
	EventInventorySucceeded   EventType = "InventorySucceeded"
	EventInventoryFailed      EventType = "InventoryFailed"
	EventInventoryCompensated EventType = "InventoryCompensated"

	EventFulfillmentStarted     EventType = "FulfillmentStarted"
	EventFulfillmentSucceeded   EventType = "FulfillmentSucceeded"
	EventFulfillmentFailed      EventType = "FulfillmentFailed"
	EventFulfillmentCompensated EventType = "FulfillmentCompensated"

	EventNotificationStarted      EventType = "NotificationStarted"
	EventNotificationSucceeded    EventType = "NotificationSucceeded"
	EventNotificationFailed       EventType = "NotificationFailed"
	EventNotificationDeadLettered EventType = "NotificationDeadLettered"
)

// WorkflowEvent represents an immutable event persisted in the event store.
type WorkflowEvent struct {
	ID         int64           `json:"id"`
	WorkflowID string          `json:"workflow_id"`
	EventType  EventType       `json:"event_type"`
	StepName   string          `json:"step_name,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	Timestamp  time.Time       `json:"timestamp"`
}

// StepEventPayload holds step execution details inside an event payload.
type StepEventPayload struct {
	StepName string          `json:"step_name,omitempty"`
	Attempt  int             `json:"attempt,omitempty"`
	Output   json.RawMessage `json:"output,omitempty"`
	Error    string          `json:"error,omitempty"`
}

// StepState holds the reconstructed state of a single step.
type StepState struct {
	Name        string          `json:"name"`
	Status      StepStatus      `json:"status"`
	Attempts    int             `json:"attempts"`
	Output      json.RawMessage `json:"output,omitempty"`
	LastError   string          `json:"last_error,omitempty"`
	Compensated bool            `json:"compensated"`
}

// WorkflowState represents the full state of a workflow reconstructed by replaying events.
type WorkflowState struct {
	WorkflowID          string                `json:"workflow_id"`
	Status              WorkflowStatus        `json:"status"`
	InitialPayload      json.RawMessage       `json:"initial_payload"`
	StepStates          map[string]*StepState `json:"step_states"`
	CompletedStepsOrder []string              `json:"completed_steps_order"`
	Events              []WorkflowEvent       `json:"events"`
	Version             int                   `json:"version"`
	CreatedAt           time.Time             `json:"created_at"`
	UpdatedAt           time.Time             `json:"updated_at"`
}

// ReplayEvents deterministically reconstructs the current WorkflowState from its chronological event log.
func ReplayEvents(workflowID string, events []WorkflowEvent) (*WorkflowState, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("no events found for workflow %s", workflowID)
	}

	state := &WorkflowState{
		WorkflowID:          workflowID,
		Status:              WorkflowStatusPending,
		StepStates:          make(map[string]*StepState),
		CompletedStepsOrder: make([]string, 0),
		Events:              events,
		Version:             len(events),
		CreatedAt:           events[0].Timestamp,
		UpdatedAt:           events[len(events)-1].Timestamp,
	}

	getOrCreateStep := func(stepName string) *StepState {
		if stepName == "" {
			return nil
		}
		st, exists := state.StepStates[stepName]
		if !exists {
			st = &StepState{
				Name:   stepName,
				Status: StepStatusPending,
			}
			state.StepStates[stepName] = st
		}
		return st
	}

	markCompletedStep := func(stepName string) {
		for _, s := range state.CompletedStepsOrder {
			if s == stepName {
				return
			}
		}
		state.CompletedStepsOrder = append(state.CompletedStepsOrder, stepName)
	}

	for _, ev := range events {
		stepName := resolveStepName(ev)
		var stepPayload StepEventPayload
		if len(ev.Payload) > 0 {
			_ = json.Unmarshal(ev.Payload, &stepPayload)
		}

		switch ev.EventType {
		case EventWorkflowStarted:
			state.Status = WorkflowStatusRunning
			state.InitialPayload = ev.Payload

		case EventStepStarted,
			EventPaymentStarted,
			EventInventoryStarted,
			EventFulfillmentStarted,
			EventNotificationStarted:
			if st := getOrCreateStep(stepName); st != nil {
				st.Status = StepStatusInProgress
				st.Attempts++
			}

		case EventStepSucceeded,
			EventPaymentSucceeded,
			EventInventorySucceeded,
			EventFulfillmentSucceeded,
			EventNotificationSucceeded:
			if st := getOrCreateStep(stepName); st != nil {
				st.Status = StepStatusSucceeded
				if len(stepPayload.Output) > 0 {
					st.Output = stepPayload.Output
				} else {
					st.Output = ev.Payload
				}
				markCompletedStep(stepName)
			}

		case EventStepRetryScheduled:
			if st := getOrCreateStep(stepName); st != nil {
				st.LastError = stepPayload.Error
			}

		case EventStepFailed,
			EventPaymentFailed,
			EventInventoryFailed,
			EventFulfillmentFailed,
			EventNotificationFailed:
			if st := getOrCreateStep(stepName); st != nil {
				st.Status = StepStatusFailed
				st.LastError = stepPayload.Error
			}

		case EventStepDeadLettered,
			EventNotificationDeadLettered:
			if st := getOrCreateStep(stepName); st != nil {
				st.Status = StepStatusDeadLettered
				st.LastError = stepPayload.Error
			}

		case EventWorkflowCompensating:
			state.Status = WorkflowStatusCompensating

		case EventStepCompensationStart:
			if st := getOrCreateStep(stepName); st != nil {
				st.Status = StepStatusCompensating
			}

		case EventStepCompensated,
			EventPaymentCompensated,
			EventInventoryCompensated,
			EventFulfillmentCompensated:
			if st := getOrCreateStep(stepName); st != nil {
				st.Status = StepStatusCompensated
				st.Compensated = true
			}

		case EventWorkflowRolledBack:
			state.Status = WorkflowStatusRolledBack

		case EventWorkflowCompleted:
			state.Status = WorkflowStatusCompleted

		case EventWorkflowFailed:
			state.Status = WorkflowStatusFailed
		}
	}

	return state, nil
}

func resolveStepName(ev WorkflowEvent) string {
	if ev.StepName != "" {
		return ev.StepName
	}
	switch ev.EventType {
	case EventPaymentStarted, EventPaymentSucceeded, EventPaymentFailed, EventPaymentCompensated:
		return "Payment"
	case EventInventoryStarted, EventInventorySucceeded, EventInventoryFailed, EventInventoryCompensated:
		return "Inventory"
	case EventFulfillmentStarted, EventFulfillmentSucceeded, EventFulfillmentFailed, EventFulfillmentCompensated:
		return "Fulfillment"
	case EventNotificationStarted, EventNotificationSucceeded, EventNotificationFailed, EventNotificationDeadLettered:
		return "Notification"
	default:
		return ""
	}
}
