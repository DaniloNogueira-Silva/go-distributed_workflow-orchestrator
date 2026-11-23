package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/limiter"
	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/metrics"
	workerv1 "github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/proto/worker/v1"
	"google.golang.org/grpc"
)

// GRPCServer implements workerv1.WorkerServiceServer and routes requests to domain step handlers.
type GRPCServer struct {
	workerv1.UnimplementedWorkerServiceServer
	dedicatedStep string
	handlers      map[string]StepHandler
	store         DomainStore
}

// NewGRPCServer constructs a gRPC Worker Server with all 4 domain handlers registered.
func NewGRPCServer(outboundLimiter *limiter.OutboundLimiter, stores ...DomainStore) *GRPCServer {
	var store DomainStore
	if len(stores) > 0 && stores[0] != nil {
		store = stores[0]
	} else {
		store = NewInMemoryDomainStore()
	}

	return &GRPCServer{
		store: store,
		handlers: map[string]StepHandler{
			"Payment":      NewPaymentWorker(outboundLimiter, store),
			"Inventory":    NewInventoryWorker(store),
			"Fulfillment":  NewFulfillmentWorker(outboundLimiter, store),
			"Notification": NewNotificationWorker(store),
		},
	}
}

// NewSingleStepGRPCServer constructs an isolated gRPC Worker Server dedicated exclusively to a single domain step
// ("Payment", "Inventory", "Fulfillment", or "Notification").
func NewSingleStepGRPCServer(stepName string, outboundLimiter *limiter.OutboundLimiter, stores ...DomainStore) (*GRPCServer, error) {
	var store DomainStore
	if len(stores) > 0 && stores[0] != nil {
		store = stores[0]
	} else {
		store = NewInMemoryDomainStore()
	}

	var handler StepHandler
	switch stepName {
	case "Payment":
		handler = NewPaymentWorker(outboundLimiter, store)
	case "Inventory":
		handler = NewInventoryWorker(store)
	case "Fulfillment":
		handler = NewFulfillmentWorker(outboundLimiter, store)
	case "Notification":
		handler = NewNotificationWorker(store)
	default:
		return nil, fmt.Errorf("unsupported worker step name: %s", stepName)
	}

	return &GRPCServer{
		dedicatedStep: stepName,
		store:         store,
		handlers: map[string]StepHandler{
			stepName: handler,
		},
	}, nil
}

// DomainStore returns the backing DomainStore used by the worker server.
func (s *GRPCServer) DomainStore() DomainStore {
	return s.store
}

// ExecuteStep handles incoming gRPC ExecuteStep calls from the Orchestrator Engine.
func (s *GRPCServer) ExecuteStep(ctx context.Context, req *workerv1.StepRequest) (*workerv1.StepResponse, error) {
	if s.dedicatedStep != "" && req.StepName != s.dedicatedStep {
		return &workerv1.StepResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("worker node dedicated to '%s' rejected request for '%s'", s.dedicatedStep, req.StepName),
		}, nil
	}

	h, exists := s.handlers[req.StepName]
	if !exists {
		return &workerv1.StepResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("unknown worker step: %s", req.StepName),
		}, nil
	}

	output, err := h.Execute(ctx, req.WorkflowId, req.Action, req.Payload)
	if err != nil {
		return &workerv1.StepResponse{
			Success:      false,
			Output:       output,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &workerv1.StepResponse{
		Success: true,
		Output:  output,
	}, nil
}

// CompensateStep handles incoming gRPC CompensateStep calls from the Orchestrator Engine.
func (s *GRPCServer) CompensateStep(ctx context.Context, req *workerv1.StepRequest) (*workerv1.StepResponse, error) {
	if s.dedicatedStep != "" && req.StepName != s.dedicatedStep {
		return &workerv1.StepResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("worker node dedicated to '%s' rejected compensation for '%s'", s.dedicatedStep, req.StepName),
		}, nil
	}

	h, exists := s.handlers[req.StepName]
	if !exists {
		return &workerv1.StepResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("unknown worker step: %s", req.StepName),
		}, nil
	}

	output, err := h.Compensate(ctx, req.WorkflowId, req.Action, req.Payload)
	if err != nil {
		return &workerv1.StepResponse{
			Success:      false,
			Output:       output,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &workerv1.StepResponse{
		Success: true,
		Output:  output,
	}, nil
}

// GRPCClientAdapter implements orchestrator.WorkerExecutor over gRPC, supporting per-step routing to isolated worker servers.
type GRPCClientAdapter struct {
	defaultClient workerv1.WorkerServiceClient
	stepClients   map[string]workerv1.WorkerServiceClient
}

// NewGRPCClientAdapter creates a WorkerExecutor backed by a single gRPC connection.
func NewGRPCClientAdapter(conn grpc.ClientConnInterface) *GRPCClientAdapter {
	return &GRPCClientAdapter{
		defaultClient: workerv1.NewWorkerServiceClient(conn),
	}
}

// NewMultiGRPCClientAdapter creates a WorkerExecutor that routes each Saga step ("Payment", "Inventory",
// "Fulfillment", "Notification") to its dedicated gRPC worker server connection.
func NewMultiGRPCClientAdapter(conns map[string]grpc.ClientConnInterface) *GRPCClientAdapter {
	clients := make(map[string]workerv1.WorkerServiceClient, len(conns))
	for stepName, conn := range conns {
		clients[stepName] = workerv1.NewWorkerServiceClient(conn)
	}
	return &GRPCClientAdapter{
		stepClients: clients,
	}
}

func (a *GRPCClientAdapter) resolveClient(stepName string) (workerv1.WorkerServiceClient, error) {
	if len(a.stepClients) > 0 {
		if client, ok := a.stepClients[stepName]; ok {
			return client, nil
		}
		return nil, fmt.Errorf("no gRPC worker connection configured for step '%s'", stepName)
	}
	if a.defaultClient != nil {
		return a.defaultClient, nil
	}
	return nil, fmt.Errorf("no gRPC client initialized for step '%s'", stepName)
}

// workerTelemetryOutput parses domain metrics and outbound delays returned from isolated worker processes
// so the main Gateway/Orchestrator Prometheus registry (:8080/metrics) reflects all cross-process worker activity.
type workerTelemetryOutput struct {
	Amount         float64  `json:"amount"`
	AmountRefunded float64  `json:"amount_refunded"`
	ItemsReserved  []string `json:"items_reserved"`
	ItemsReleased  []string `json:"items_released"`
	OutboundDelays int      `json:"outbound_delays"`
}

func recordCrossProcessWorkerMetrics(stepName string, isCompensation bool, succeeded bool, rawOutput []byte) {
	var out workerTelemetryOutput
	if len(rawOutput) > 0 {
		_ = json.Unmarshal(rawOutput, &out)
	}

	switch stepName {
	case "Payment":
		if out.OutboundDelays > 0 {
			metrics.OutboundRateLimitDelaysTotal.WithLabelValues("payment_gateway").Add(float64(out.OutboundDelays))
		}
		if succeeded {
			if !isCompensation && out.Amount > 0 {
				metrics.DomainPaymentVolumeTotal.WithLabelValues("charged").Add(out.Amount)
			} else if isCompensation && out.AmountRefunded > 0 {
				metrics.DomainPaymentVolumeTotal.WithLabelValues("refunded").Add(out.AmountRefunded)
			}
		}
	case "Inventory":
		if succeeded {
			if !isCompensation && len(out.ItemsReserved) > 0 {
				metrics.DomainInventoryUnitsTotal.WithLabelValues("reserved").Add(float64(len(out.ItemsReserved)))
			} else if isCompensation && len(out.ItemsReleased) > 0 {
				metrics.DomainInventoryUnitsTotal.WithLabelValues("released").Add(float64(len(out.ItemsReleased)))
			}
		}
	case "Fulfillment":
		if out.OutboundDelays > 0 {
			metrics.OutboundRateLimitDelaysTotal.WithLabelValues("shipping_carrier").Add(float64(out.OutboundDelays))
		}
		if succeeded {
			if !isCompensation {
				metrics.DomainShipmentsTotal.WithLabelValues("generated").Inc()
			} else {
				metrics.DomainShipmentsTotal.WithLabelValues("voided").Inc()
			}
		}
	case "Notification":
		if succeeded && !isCompensation {
			metrics.DomainNotificationsSentTotal.Inc()
		}
	}
}

// ExecuteStep invokes the remote WorkerService.ExecuteStep RPC on the target domain worker node.
func (a *GRPCClientAdapter) ExecuteStep(ctx context.Context, workflowID, stepName, action string, payload json.RawMessage) (json.RawMessage, error) {
	client, err := a.resolveClient(stepName)
	if err != nil {
		return nil, err
	}

	resp, err := client.ExecuteStep(ctx, &workerv1.StepRequest{
		WorkflowId: workflowID,
		StepName:   stepName,
		Action:     action,
		Payload:    payload,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC ExecuteStep transport error (%s): %w", stepName, err)
	}
	if !resp.Success {
		recordCrossProcessWorkerMetrics(stepName, false, false, resp.Output)
		return nil, errors.New(resp.ErrorMessage)
	}

	recordCrossProcessWorkerMetrics(stepName, false, true, resp.Output)
	return json.RawMessage(resp.Output), nil
}

// CompensateStep invokes the remote WorkerService.CompensateStep RPC on the target domain worker node.
func (a *GRPCClientAdapter) CompensateStep(ctx context.Context, workflowID, stepName, compensationAction string, payload json.RawMessage) (json.RawMessage, error) {
	client, err := a.resolveClient(stepName)
	if err != nil {
		return nil, err
	}

	resp, err := client.CompensateStep(ctx, &workerv1.StepRequest{
		WorkflowId: workflowID,
		StepName:   stepName,
		Action:     compensationAction,
		Payload:    payload,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC CompensateStep transport error (%s): %w", stepName, err)
	}
	if !resp.Success {
		recordCrossProcessWorkerMetrics(stepName, true, false, resp.Output)
		return nil, errors.New(resp.ErrorMessage)
	}

	recordCrossProcessWorkerMetrics(stepName, true, true, resp.Output)
	return json.RawMessage(resp.Output), nil
}
