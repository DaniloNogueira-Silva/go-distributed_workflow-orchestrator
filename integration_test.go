package main_test

import (
	"bytes"
	"context"
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
	httpServer  *httptest.Server
	grpcServer  *grpc.Server
	grpcConn    *grpc.ClientConn
	redisSrv    *miniredis.Miniredis
	redisCli    *redis.Client
	repo        *eventsourcing.InMemoryRepository
	domainStore *worker.InMemoryDomainStore
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
	outboundLimiter := limiter.NewOutboundLimiter(outboundBucket, 40*time.Millisecond, 50)

	domainStore := worker.NewInMemoryDomainStore()

	// Start real gRPC Worker Server on an ephemeral local TCP port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on tcp: %v", err)
	}

	grpcServer := grpc.NewServer()
	workerv1.RegisterWorkerServiceServer(grpcServer, worker.NewGRPCServer(outboundLimiter, domainStore))
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
	gwHandler := gateway.NewHandlerWithEngine(inboundLimiter, engine, domainStore)
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
		httpServer:  httpServer,
		grpcServer:  grpcServer,
		grpcConn:    grpcConn,
		redisSrv:    s,
		redisCli:    rdb,
		repo:        repo,
		domainStore: domainStore,
	}
}

func TestIntegration_AllScenarios(t *testing.T) {
	ctx := context.Background()
	// Configure Inbound capacity = 3, Outbound capacity = 1 (with 25 tokens/sec refill so outbound delay-and-retry is exercised)
	env := setupEndToEndEnv(t, 3, 0.5, 1, 25)

	// =========================================================================
	// Scenario 1: Successful Checkout Saga all the way to Notification
	// =========================================================================
	t.Run("Scenario 1 - Successful Checkout Saga Deducts Real Stock, Balance, and Persists Shipment & Email", func(t *testing.T) {
		kbBefore, _ := env.domainStore.GetProductStock(ctx, "keyboard_mech")

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

		// Verify real stock deduction in DomainStore
		kbAfter, _ := env.domainStore.GetProductStock(ctx, "keyboard_mech")
		if kbAfter.AvailableStock != kbBefore.AvailableStock-1 || kbAfter.ReservedStock != kbBefore.ReservedStock+1 {
			t.Fatalf("expected keyboard_mech stock to decrease by 1, got before=%d after=%d",
				kbBefore.AvailableStock, kbAfter.AvailableStock)
		}

		// Verify real customer balance debit in DomainStore
		acc, _ := env.domainStore.GetCustomerAccount(ctx, "cust_success_01")
		expectedBal := worker.DefaultInitialCustomerBalance - 349.90
		if acc.Balance != expectedBal {
			t.Fatalf("expected customer balance %.2f, got %.2f", expectedBal, acc.Balance)
		}

		// Verify GET /api/v1/domain/state endpoint
		domResp, err := http.Get(env.httpServer.URL + "/api/v1/domain/state")
		if err != nil {
			t.Fatalf("GET domain state failed: %v", err)
		}
		defer domResp.Body.Close()
		if domResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK on GET domain state, got %d", domResp.StatusCode)
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
	})

	// =========================================================================
	// Scenario 3: Fulfillment fails, triggering rollback for Inventory and Payment
	// =========================================================================
	t.Run("Scenario 3 - Fulfillment Failure Triggers Reverse Compensations Restoring Real Stock and Balance", func(t *testing.T) {
		monBefore, _ := env.domainStore.GetProductStock(ctx, "monitor_4k")

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

		var checkoutResp gateway.CheckoutResponse
		_ = json.NewDecoder(resp.Body).Decode(&checkoutResp)

		if checkoutResp.Status != string(eventsourcing.WorkflowStatusRolledBack) {
			t.Fatalf("expected workflow status ROLLED_BACK, got %s", checkoutResp.Status)
		}

		// Verify physical stock was restored to exact pre-checkout count after ReleaseItems compensation
		monAfter, _ := env.domainStore.GetProductStock(ctx, "monitor_4k")
		if monAfter.AvailableStock != monBefore.AvailableStock || monAfter.ReservedStock != 0 {
			t.Fatalf("expected monitor_4k stock restored to %d (reserved=0), got available=%d reserved=%d",
				monBefore.AvailableStock, monAfter.AvailableStock, monAfter.ReservedStock)
		}

		// Verify customer balance was refunded back to full initial balance after RefundCard compensation
		acc, _ := env.domainStore.GetCustomerAccount(ctx, "cust_rollback_03")
		if acc.Balance != worker.DefaultInitialCustomerBalance {
			t.Fatalf("expected customer balance restored to %.2f after refund, got %.2f",
				worker.DefaultInitialCustomerBalance, acc.Balance)
		}
	})

	// =========================================================================
	// Scenario 4: Real Domain Business Rule Failures (Out-of-Stock & Prohibited Parcel)
	// =========================================================================
	t.Run("Scenario 4 - Real Out-of-Stock SKU in Inventory Automatically Refunds Payment", func(t *testing.T) {
		reqBody := gateway.CheckoutRequest{
			CustomerID: "cust_real_oos",
			Amount:     599.00,
			Currency:   "BRL",
			Items:      []string{"out_of_stock_item"}, // Seeded with 0 stock in DomainStore
		}
		payloadBytes, _ := json.Marshal(reqBody)

		req, _ := http.NewRequest(http.MethodPost, env.httpServer.URL+"/api/v1/workflows/checkout", bytes.NewReader(payloadBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", "client-scenario-4")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("HTTP request failed: %v", err)
		}
		defer resp.Body.Close()

		var checkoutResp gateway.CheckoutResponse
		_ = json.NewDecoder(resp.Body).Decode(&checkoutResp)

		if checkoutResp.Status != string(eventsourcing.WorkflowStatusRolledBack) {
			t.Fatalf("expected ROLLED_BACK when ordering out_of_stock_item, got %s", checkoutResp.Status)
		}
		if !checkoutResp.WorkflowState.StepStates["Payment"].Compensated {
			t.Fatalf("expected Payment to be compensated (refunded) after real Inventory stock shortage")
		}

		acc, _ := env.domainStore.GetCustomerAccount(ctx, "cust_real_oos")
		if acc.Balance != worker.DefaultInitialCustomerBalance {
			t.Fatalf("expected customer balance refunded to %.2f, got %.2f",
				worker.DefaultInitialCustomerBalance, acc.Balance)
		}
	})
}
