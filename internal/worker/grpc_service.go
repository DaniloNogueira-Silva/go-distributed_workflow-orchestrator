package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	workerv1 "github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/proto/worker/v1"
	"google.golang.org/grpc"
)

// GRPCServer implements workerv1.WorkerServiceServer and routes requests to the 4 domain workers.
type GRPCServer struct {
	workerv1.UnimplementedWorkerServiceServer
	handlers map[string]StepHandler
}

// NewGRPCServer constructs a gRPC Worker Server with Payment, Inventory, Fulfillment, and Notification workers.
func NewGRPCServer(outboundLimiter *limiter.OutboundLimiter) *GRPCServer {
	return &GRPCServer{
		handlers: map[string]StepHandler{
			"Payment":      NewPaymentWorker(outboundLimiter),
			"Inventory":    NewInventoryWorker(),
			"Fulfillment":  NewFulfillmentWorker(outboundLimiter),
			"Notification": NewNotificationWorker(),
		},
	}
}

// ExecuteStep handles incoming gRPC ExecuteStep calls from the Orchestrator Engine.
func (s *GRPCServer) ExecuteStep(ctx context.Context, req *workerv1.StepRequest) (*workerv1.StepResponse, error) {
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
			ErrorMessage: err.Error(),
		}, nil
	}

	return &workerv1.StepResponse{
		Success: true,
		Output:  output,
	}, nil
}

// GRPCClientAdapter implements orchestrator.WorkerExecutor over gRPC.
type GRPCClientAdapter struct {
	client workerv1.WorkerServiceClient
}

// NewGRPCClientAdapter creates a WorkerExecutor backed by a gRPC connection.
func NewGRPCClientAdapter(conn grpc.ClientConnInterface) *GRPCClientAdapter {
	return &GRPCClientAdapter{
		client: workerv1.NewWorkerServiceClient(conn),
	}
}

// ExecuteStep invokes the remote WorkerService.ExecuteStep RPC.
func (a *GRPCClientAdapter) ExecuteStep(ctx context.Context, workflowID, stepName, action string, payload json.RawMessage) (json.RawMessage, error) {
	resp, err := a.client.ExecuteStep(ctx, &workerv1.StepRequest{
		WorkflowId: workflowID,
		StepName:   stepName,
		Action:     action,
		Payload:    payload,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC ExecuteStep transport error: %w", err)
	}
	if !resp.Success {
		return nil, errors.New(resp.ErrorMessage)
	}
	return json.RawMessage(resp.Output), nil
}

// CompensateStep invokes the remote WorkerService.CompensateStep RPC.
func (a *GRPCClientAdapter) CompensateStep(ctx context.Context, workflowID, stepName, compensationAction string, payload json.RawMessage) (json.RawMessage, error) {
	resp, err := a.client.CompensateStep(ctx, &workerv1.StepRequest{
		WorkflowId: workflowID,
		StepName:   stepName,
		Action:     compensationAction,
		Payload:    payload,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC CompensateStep transport error: %w", err)
	}
	if !resp.Success {
		return nil, errors.New(resp.ErrorMessage)
	}
	return json.RawMessage(resp.Output), nil
}
