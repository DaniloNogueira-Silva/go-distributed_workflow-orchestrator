package workerv1

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func init() {
	// Register a hybrid proto/JSON codec under "proto" so standard gRPC servers and clients
	// serialize both generated proto.Message types and plain Go structs seamlessly.
	encoding.RegisterCodec(hybridProtoCodec{})
}

type hybridProtoCodec struct{}

func (hybridProtoCodec) Marshal(v interface{}) ([]byte, error) {
	if pm, ok := v.(proto.Message); ok {
		return proto.Marshal(pm)
	}
	return json.Marshal(v)
}

func (hybridProtoCodec) Unmarshal(data []byte, v interface{}) error {
	if pm, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, pm)
	}
	return json.Unmarshal(data, v)
}

func (hybridProtoCodec) Name() string {
	return "proto"
}

// StepRequest represents a request from the Orchestrator Engine to execute or compensate a step.
type StepRequest struct {
	WorkflowId string `json:"workflow_id"`
	StepName   string `json:"step_name"`
	Action     string `json:"action"`
	Payload    []byte `json:"payload"`
}

// StepResponse represents the result returned by a Worker.
type StepResponse struct {
	Success      bool   `json:"success"`
	Output       []byte `json:"output"`
	ErrorMessage string `json:"error_message"`
}

// WorkerServiceClient is the client API for WorkerService service.
type WorkerServiceClient interface {
	ExecuteStep(ctx context.Context, in *StepRequest, opts ...grpc.CallOption) (*StepResponse, error)
	CompensateStep(ctx context.Context, in *StepRequest, opts ...grpc.CallOption) (*StepResponse, error)
}

type workerServiceClient struct {
	cc grpc.ClientConnInterface
}

// NewWorkerServiceClient creates a new gRPC client for WorkerService.
func NewWorkerServiceClient(cc grpc.ClientConnInterface) WorkerServiceClient {
	return &workerServiceClient{cc}
}

func (c *workerServiceClient) ExecuteStep(ctx context.Context, in *StepRequest, opts ...grpc.CallOption) (*StepResponse, error) {
	out := new(StepResponse)
	err := c.cc.Invoke(ctx, "/worker.v1.WorkerService/ExecuteStep", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *workerServiceClient) CompensateStep(ctx context.Context, in *StepRequest, opts ...grpc.CallOption) (*StepResponse, error) {
	out := new(StepResponse)
	err := c.cc.Invoke(ctx, "/worker.v1.WorkerService/CompensateStep", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WorkerServiceServer is the server API for WorkerService service.
type WorkerServiceServer interface {
	ExecuteStep(context.Context, *StepRequest) (*StepResponse, error)
	CompensateStep(context.Context, *StepRequest) (*StepResponse, error)
}

// UnimplementedWorkerServiceServer can be embedded to have forward compatible implementations.
type UnimplementedWorkerServiceServer struct{}

func (UnimplementedWorkerServiceServer) ExecuteStep(context.Context, *StepRequest) (*StepResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ExecuteStep not implemented")
}

func (UnimplementedWorkerServiceServer) CompensateStep(context.Context, *StepRequest) (*StepResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CompensateStep not implemented")
}

// RegisterWorkerServiceServer registers the WorkerService implementation with a grpc.Server.
func RegisterWorkerServiceServer(s grpc.ServiceRegistrar, srv WorkerServiceServer) {
	s.RegisterService(&WorkerService_ServiceDesc, srv)
}

func _WorkerService_ExecuteStep_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StepRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).ExecuteStep(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/worker.v1.WorkerService/ExecuteStep",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerServiceServer).ExecuteStep(ctx, req.(*StepRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WorkerService_CompensateStep_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StepRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WorkerServiceServer).CompensateStep(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/worker.v1.WorkerService/CompensateStep",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WorkerServiceServer).CompensateStep(ctx, req.(*StepRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// WorkerService_ServiceDesc is the grpc.ServiceDesc for WorkerService service.
var WorkerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "worker.v1.WorkerService",
	HandlerType: (*WorkerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ExecuteStep",
			Handler:    _WorkerService_ExecuteStep_Handler,
		},
		{
			MethodName: "CompensateStep",
			Handler:    _WorkerService_CompensateStep_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/worker/v1/worker.proto",
}
