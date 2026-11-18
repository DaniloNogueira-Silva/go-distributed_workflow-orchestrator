package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal tracks all HTTP requests hitting the Edge API Gateway.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_http_requests_total",
		Help: "Total number of HTTP requests processed by the Edge API Gateway.",
	}, []string{"method", "endpoint", "status"})

	// HTTPRequestDuration tracks latency of HTTP requests at the Edge API Gateway.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orchestrator_http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	}, []string{"endpoint", "status"})

	// InboundRateLimitTotal tracks decisions made by the Edge Token Bucket Rate Limiter.
	InboundRateLimitTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_inbound_rate_limit_total",
		Help: "Total inbound rate limit evaluations partitioned by decision (allowed vs blocked).",
	}, []string{"api_key", "decision"})

	// WorkflowsTotal tracks total workflows finished by terminal status (COMPLETED, ROLLED_BACK, FAILED).
	WorkflowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_workflows_total",
		Help: "Total number of Saga workflows completed, rolled back, or failed.",
	}, []string{"status"})

	// WorkflowsInFlight tracks the number of currently active workflows executing.
	WorkflowsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "orchestrator_workflows_in_flight",
		Help: "Current number of Saga workflows actively executing.",
	})

	// WorkflowDuration tracks the total execution time of a Saga workflow.
	WorkflowDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orchestrator_workflow_duration_seconds",
		Help:    "End-to-end execution time of Saga workflows in seconds.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	}, []string{"status"})

	// SagaStepExecutionsTotal tracks forward executions and compensations for each step.
	SagaStepExecutionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_saga_step_executions_total",
		Help: "Total number of Saga step executions and compensations by step, action, and outcome.",
	}, []string{"step", "action", "outcome"})

	// SagaStepDuration tracks latency of individual gRPC worker step calls.
	SagaStepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orchestrator_saga_step_duration_seconds",
		Help:    "Execution duration of individual Saga steps over gRPC in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"step", "operation"})

	// OutboundRateLimitDelaysTotal tracks how many times workers had to delay and retry due to outbound rate limiting.
	OutboundRateLimitDelaysTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_outbound_rate_limit_delays_total",
		Help: "Total number of outbound rate limiter delay-and-retry waits by external service.",
	}, []string{"service"})

	// OutboundRateLimitRequestsTotal tracks outbound rate limit checks (allowed immediately vs delayed).
	OutboundRateLimitRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_outbound_rate_limit_requests_total",
		Help: "Total outbound calls guarded by the Outbound Rate Limiter.",
	}, []string{"service", "throttled"})

	// DLQMessagesTotal tracks messages routed to the Dead Letter Queue.
	DLQMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_dlq_messages_total",
		Help: "Total number of failed non-critical steps sent to the Dead Letter Queue.",
	}, []string{"step"})

	// EventsPersistedTotal tracks every domain event appended to the Event Store.
	EventsPersistedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orchestrator_events_persisted_total",
		Help: "Total number of Event Sourcing events persisted by event_type.",
	}, []string{"event_type"})
)
