package orchestrator

import (
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/eventsourcing"
)

// FailurePolicy dictates how the Orchestrator handles a step failure.
type FailurePolicy string

const (
	// PolicyFailAndCompensate stops forward execution and triggers reverse compensations for completed steps.
	PolicyFailAndCompensate FailurePolicy = "FAIL_AND_COMPENSATE"
	// PolicyRetryThenDLQ retries with exponential backoff; if exhausted, routes to Dead Letter Queue without failing the workflow.
	PolicyRetryThenDLQ FailurePolicy = "RETRY_THEN_DLQ"
)

// StepDefinition defines a single step in a Saga Workflow Blueprint.
type StepDefinition struct {
	Name               string
	Action             string
	CompensationAction string
	HasCompensation    bool
	FailurePolicy      FailurePolicy
	MaxRetries         int
	InitialBackoff     time.Duration
	StartEvent         eventsourcing.EventType
	SuccessEvent       eventsourcing.EventType
	FailEvent          eventsourcing.EventType
	CompensateEvent    eventsourcing.EventType
	DLQEvent           eventsourcing.EventType
}

// WorkflowBlueprint defines an ordered sequence of Saga steps.
type WorkflowBlueprint struct {
	Name  string
	Steps []StepDefinition
}

// StepByName looks up a StepDefinition by name.
func (wb *WorkflowBlueprint) StepByName(name string) (StepDefinition, bool) {
	for _, s := range wb.Steps {
		if s.Name == name {
			return s, true
		}
	}
	return StepDefinition{}, false
}

// NewCheckoutSagaBlueprint creates the canonical 4-step E-commerce Checkout Saga Blueprint:
// Payment -> Inventory -> Fulfillment -> Notification.
func NewCheckoutSagaBlueprint(notificationBackoff time.Duration) *WorkflowBlueprint {
	if notificationBackoff <= 0 {
		notificationBackoff = 50 * time.Millisecond
	}

	return &WorkflowBlueprint{
		Name: "CHECKOUT_SAGA",
		Steps: []StepDefinition{
			{
				Name:               "Payment",
				Action:             "ChargeCard",
				CompensationAction: "RefundCard",
				HasCompensation:    true,
				FailurePolicy:      PolicyFailAndCompensate,
				MaxRetries:         1,
				StartEvent:         eventsourcing.EventPaymentStarted,
				SuccessEvent:       eventsourcing.EventPaymentSucceeded,
				FailEvent:          eventsourcing.EventPaymentFailed,
				CompensateEvent:    eventsourcing.EventPaymentCompensated,
			},
			{
				Name:               "Inventory",
				Action:             "ReserveItems",
				CompensationAction: "ReleaseItems",
				HasCompensation:    true,
				FailurePolicy:      PolicyFailAndCompensate,
				MaxRetries:         1,
				StartEvent:         eventsourcing.EventInventoryStarted,
				SuccessEvent:       eventsourcing.EventInventorySucceeded,
				FailEvent:          eventsourcing.EventInventoryFailed,
				CompensateEvent:    eventsourcing.EventInventoryCompensated,
			},
			{
				Name:               "Fulfillment",
				Action:             "GenerateShippingLabel",
				CompensationAction: "VoidLabel",
				HasCompensation:    true,
				FailurePolicy:      PolicyFailAndCompensate,
				MaxRetries:         1,
				StartEvent:         eventsourcing.EventFulfillmentStarted,
				SuccessEvent:       eventsourcing.EventFulfillmentSucceeded,
				FailEvent:          eventsourcing.EventFulfillmentFailed,
				CompensateEvent:    eventsourcing.EventFulfillmentCompensated,
			},
			{
				Name:               "Notification",
				Action:             "SendEmail",
				CompensationAction: "",
				HasCompensation:    false,
				FailurePolicy:      PolicyRetryThenDLQ,
				MaxRetries:         3,
				InitialBackoff:     notificationBackoff,
				StartEvent:         eventsourcing.EventNotificationStarted,
				SuccessEvent:       eventsourcing.EventNotificationSucceeded,
				FailEvent:          eventsourcing.EventNotificationFailed,
				DLQEvent:           eventsourcing.EventNotificationDeadLettered,
			},
		},
	}
}
