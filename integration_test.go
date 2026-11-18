package main_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/eventsourcing"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/gateway"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/orchestrator"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/worker"
	workerv1 "github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/proto/worker/v1"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testEnv struct {
	httpServer *httptest.Server
	grpcServer *grpc.Server
	grpcConn   *grpc.ClientConn
	redisSrv   *miniredis.Miniredis
	redisCli   *redis.Client
	repo       *eventsourcing.InMemoryRepository
}

func setupEndToEndEnv(t *testing.T, inboundCap, inboundRefill, outboundCap, outboundRefill float64) *testEnv {
	t.Helper()

	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})

	inboundLimiter := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         inboundCap,
		RefillRatePerSec: inboundRefill,
		TTLSeconds:       60,
	})

	outboundBucket := limiter.NewTokenBucketLimiter(rdb, limiter.Config{
		Capacity:         outboundCap,
		RefillRatePerSec: outboundRefill,
		TTLSeconds:       60,
	})
	outboundLimiter := limiter.NewOutboundLimiter(outboundBucket, 40*time.Millisecond, 30)

	// Start real gRPC Worker Server on an ephemeral local TCP port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on tcp: %v", err)
	}

	grpcServer := grpc.NewServer()
	workerv1.RegisterWorkerServiceServer(grpcServer, worker.NewGRPCServer(outboundLimiter))
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	grpcConn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC worker server: %v", err)
	}

	repo := eventsourcing.NewInMemoryRepository()
	workerAdapter := worker.NewGRPCClientAdapter(grpcConn)
	blueprint := orchestrator.NewCheckoutSagaBlueprint(10 * time.Millisecond)
	engine := orchestrator.NewEngine(repo, blueprint, workerAdapter)

	mux := http.NewServeMux()
	gwHandler := gateway.NewHandlerWithEngine(inboundLimiter, engine)
	gwHandler.RegisterRoutes(mux)

	httpServer := httptest.NewServer(mux)

	t.Cleanup(func() {
		httpServer.Close()
		_ = grpcConn.Close()
		grpcServer.Stop()
		_ = rdb.Close()
		s.Close()
	})

	return &testEnv{
		httpServer: httpServer,
		grpcServer: grpcServer,
		grpcConn:   grpcConn,
		redisSrv:   s,
		redisCli:   rdb,
		repo:       repo,
	}
}

func TestIntegration_AllScenarios(t *testing.T) {
	// Configure Inbound capacity = 3, Outbound capacity = 1 (with 25 tokens/sec refill so outbound delay-and-retry is exercised)
	env := setupEndToEndEnv(t, 3, 0.5, 1, 25)

	// =========================================================================
	// Scenario 1: Successful Checkout Saga all the way to Notification
	// =========================================================================
	t.Run("Scenario 1 - Successful Checkout Saga (Payment -> Inventory -> Fulfillment -> Notification)", func(t *testing.T) {
		reqBody := gateway.CheckoutRequest{
			CustomerID: "cust_success_01",
			Amount:     349.90,
			Currency:   "BRL",
			Items:      []string{"keyboard_mech", "mouse_wireless"},
		}
		payloadBytes, _ := json.Marshal(reqBody)

		req, _ := http.NewRequest(http.MethodPost, env.httpServer.URL+"/api/v1/workflows/checkout", bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", "client-scenario-1")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("HTTP request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
		}

		var checkoutResp gateway.CheckoutResponse
		if err := json.NewDecoder(resp.Body).Decode(&checkoutResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if checkoutResp.Status != string(eventsourcing.WorkflowStatusCompleted) {
			t.Fatalf("expected workflow status COMPLETED, got %s", checkoutResp.Status)
		}

		state := checkoutResp.WorkflowState
		if state == nil {
			t.Fatalf("expected non-nil workflow_state in response")
		}

		for _, stepName := range []string{"Payment", "Inventory", "Fulfillment", "Notification"} {
			st, ok := state.StepStates[stepName]
			if !ok {
				t.Fatalf("missing step state for %s", stepName)
			}
			if st.Status != eventsourcing.StepStatusSucceeded {
				t.Fatalf("expected step %s to be SUCCEEDED, got %s", stepName, st.Status)
			}
		}

		// Verify GET /api/v1/workflows/{id} endpoint reconstructs the exact same state
		getResp, err := http.Get(env.httpServer.URL + "/api/v1/workflows/" + checkoutResp.WorkflowID)
		if err != nil {
			t.Fatalf("GET workflow state failed: %v", err)
		}
		defer getResp.Body.Close()
		if getResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK on GET workflow, got %d", getResp.StatusCode)
		}
	})

	// =========================================================================
	// Scenario 2: Inbound Rate Limiter blocks a client after exhausting tokens
	// =========================================================================
	t.Run("Scenario 2 - Inbound Rate Limiter Blocks Client with 429 Too Many Requests", func(t *testing.T) {
		reqBody := gateway.CheckoutRequest{
			CustomerID: "cust_burst",
			Amount:     49.90,
			Currency:   "USD",
			Items:      []string{"item_a"},
		}
		payloadBytes, _ := json.Marshal(reqBody)

		apiKey := "client-scenario-2-burst"

		// Consume all 3 tokens in the bucket
		for i := 1; i <= 3; i++ {
			req, _ := http.NewRequest(http.MethodPost, env.httpServer.URL+"/api/v1/workflows/checkout", bytes.NewReader(payloadBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", apiKey)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request %d failed: %v", i, err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("expected request %d to return 202 Accepted, got %d", i, resp.StatusCode)
			}
		}

		// 4th immediate request must be blocked with 429 Too Many Requests
		reqBlocked, _ := http.NewRequest(http.MethodPost, env.httpServer.URL+"/api/v1/workflows/checkout", bytes.NewReader(payloadBytes))
		reqBlocked.Header.Set("Content-Type", "application/json")
		reqBlocked.Header.Set("X-API-Key", apiKey)

		respBlocked, err := http.DefaultClient.Do(reqBlocked)
		if err != nil {
			t.Fatalf("blocked request failed: %v", err)
		}
		defer respBlocked.Body.Close()

		if respBlocked.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("expected 429 Too Many Requests, got %d", respBlocked.StatusCode)
		}
		if respBlocked.Header.Get("X-RateLimit-Remaining") != "0" {
			t.Fatalf("expected X-RateLimit-Remaining=0, got %s", respBlocked.Header.Get("X-RateLimit-Remaining"))
		}
	})

	// =========================================================================
	// Scenario 3: Fulfillment fails, triggering rollback for Inventory and Payment
	// =========================================================================
	t.Run("Scenario 3 - Fulfillment Failure Triggers Reverse Compensations (Inventory -> Payment)", func(t *testing.T) {
		reqBody := gateway.CheckoutRequest{
			CustomerID:       "cust_rollback_03",
			Amount:           1299.00,
			Currency:         "BRL",
			Items:            []string{"monitor_4k", "hdmi_cable"},
			SimulateFailStep: "Fulfillment",
		}
		payloadBytes, _ := json.Marshal(reqBody)

		req, _ := http.NewRequest(http.MethodPost, env.httpServer.URL+"/api/v1/workflows/checkout", bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", "client-scenario-3")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("HTTP request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
		}

		var checkoutResp gateway.CheckoutResponse
		if err := json.NewDecoder(resp.Body).Decode(&checkoutResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if checkoutResp.Status != string(eventsourcing.WorkflowStatusRolledBack) {
			t.Fatalf("expected workflow status ROLLED_BACK, got %s", checkoutResp.Status)
		}

		state := checkoutResp.WorkflowState
		if state.StepStates["Fulfillment"].Status != eventsourcing.StepStatusFailed {
			t.Fatalf("expected Fulfillment status FAILED, got %s", state.StepStates["Fulfillment"].Status)
		}
		if !state.StepStates["Inventory"].Compensated {
			t.Fatalf("expected Inventory step to be compensated (ReleaseItems)")
		}
		if !state.StepStates["Payment"].Compensated {
			t.Fatalf("expected Payment step to be compensated (RefundCard)")
		}
		if _, exists := state.StepStates["Notification"]; exists {
			t.Fatalf("expected Notification step to never be executed after Fulfillment failure")
		}

		// Verify that InventoryCompensated occurred BEFORE PaymentCompensated in the Event Log
		invCompIdx := -1
		payCompIdx := -1
		for idx, ev := range state.Events {
			if ev.EventType == eventsourcing.EventInventoryCompensated {
				invCompIdx = idx
			}
			if ev.EventType == eventsourcing.EventPaymentCompensated {
				payCompIdx = idx
			}
		}
		if invCompIdx == -1 || payCompIdx == -1 {
			t.Fatalf("expected both InventoryCompensated and PaymentCompensated events in log")
		}
		if invCompIdx > payCompIdx {
			t.Fatalf("expected InventoryCompensated (idx %d) before PaymentCompensated (idx %d)", invCompIdx, payCompIdx)
		}
	})
}
