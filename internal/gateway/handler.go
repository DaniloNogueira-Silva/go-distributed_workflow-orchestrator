package gateway

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/google/uuid"
)

// CheckoutRequest represents the incoming payload to initiate a checkout saga.
type CheckoutRequest struct {
	CustomerID string   `json:"customer_id"`
	Amount     float64  `json:"amount"`
	Currency   string   `json:"currency"`
	Items      []string `json:"items"`
}

// CheckoutResponse represents the response when a checkout workflow is accepted.
type CheckoutResponse struct {
	WorkflowID string    `json:"workflow_id"`
	Status     string    `json:"status"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}

// ErrorResponse standardizes error responses.
type ErrorResponse struct {
	Error             string `json:"error"`
	Message           string `json:"message"`
	RetryAfterSeconds int    `json:"retry_after_seconds,omitempty"`
}

// Handler handles Edge API Gateway routes.
type Handler struct {
	limiter *limiter.TokenBucketLimiter
}

// NewHandler creates a new gateway Handler.
func NewHandler(l *limiter.TokenBucketLimiter) *Handler {
	return &Handler{
		limiter: l,
	}
}

// RegisterRoutes registers HTTP routes onto a standard http.ServeMux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.HealthCheck)
	mux.HandleFunc("POST /api/v1/workflows/checkout", h.HandleCheckout)
}

// HealthCheck provides service health status.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// HandleCheckout validates the API key, checks rate limits, and accepts checkout workflows.
func (h *Handler) HandleCheckout(w http.ResponseWriter, r *http.Request) {
	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if apiKey == "" {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error:   "Unauthorized",
			Message: "Missing or empty 'X-API-Key' header",
		})
		return
	}

	rateLimitKey := "rate:inbound:" + apiKey
	result, err := h.limiter.Allow(r.Context(), rateLimitKey)
	if err != nil {
		log.Printf("[Gateway Error] Rate limiter error for key %s: %v", apiKey, err)
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "Internal Server Error",
			Message: "Failed to evaluate rate limits",
		})
		return
	}

	if !result.Allowed {
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-RateLimit-Remaining", "0")
		writeJSON(w, http.StatusTooManyRequests, ErrorResponse{
			Error:             "Too Many Requests",
			Message:           "Rate limit exceeded. Token bucket depleted.",
			RetryAfterSeconds: 1,
		})
		return
	}

	// Rate limit passed
	w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(result.Remaining, 10))

	var req CheckoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "Bad Request",
			Message: "Invalid JSON request payload",
		})
		return
	}

	// Generate workflow ID (in Phase 2 and 3, this will be dispatched to the Orchestrator Engine)
	workflowID := "wf_" + uuid.NewString()

	resp := CheckoutResponse{
		WorkflowID: workflowID,
		Status:     "ACCEPTED",
		Message:    "Checkout saga workflow accepted for execution",
		CreatedAt:  time.Now().UTC(),
	}

	log.Printf("[Gateway] Accepted workflow %s for customer %s (Remaining tokens: %d)",
		workflowID, req.CustomerID, result.Remaining)

	writeJSON(w, http.StatusAccepted, resp)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
