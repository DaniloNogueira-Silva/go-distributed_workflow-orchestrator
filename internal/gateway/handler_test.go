package gateway_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/gateway"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestGateway_HandleCheckout(t *testing.T) {
	s := miniredis.RunT(t)
	defer s.Close()

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer client.Close()

	tb := limiter.NewTokenBucketLimiter(client, limiter.Config{
		Capacity:         2,
		RefillRatePerSec: 1,
		TTLSeconds:       60,
	})

	handler := gateway.NewHandler(tb)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	payload := gateway.CheckoutRequest{
		CustomerID: "cust_abc",
		Amount:     99.90,
		Currency:   "BRL",
		Items:      []string{"item-1"},
	}
	body, _ := json.Marshal(payload)

	// 1. Missing API Key should yield 401
	reqNoKey := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/checkout", bytes.NewReader(body))
	recNoKey := httptest.NewRecorder()
	mux.ServeHTTP(recNoKey, reqNoKey)

	if recNoKey.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", recNoKey.Code)
	}

	// 2. Request 1 with API Key should yield 202 Accepted
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/checkout", bytes.NewReader(body))
	req1.Header.Set("X-API-Key", "api-key-123")
	rec1 := httptest.NewRecorder()
	mux.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec1.Code, rec1.Body.String())
	}

	// 3. Request 2 with API Key should yield 202 Accepted
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/checkout", bytes.NewReader(body))
	req2.Header.Set("X-API-Key", "api-key-123")
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec2.Code, rec2.Body.String())
	}

	// 4. Request 3 with API Key should yield 429 Too Many Requests
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/checkout", bytes.NewReader(body))
	req3.Header.Set("X-API-Key", "api-key-123")
	rec3 := httptest.NewRecorder()
	mux.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d: %s", rec3.Code, rec3.Body.String())
	}

	if rec3.Header().Get("Retry-After") != "1" {
		t.Fatalf("expected Retry-After header to be '1', got %s", rec3.Header().Get("Retry-After"))
	}
}
