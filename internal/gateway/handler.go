package gateway

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/eventsourcing"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/orchestrator"
	"github.com/google/uuid"
)

// CheckoutRequest represents the incoming payload to initiate a checkout saga.
type CheckoutRequest struct {
	CustomerID       string   `json:"customer_id"`
	Amount           float64  `json:"amount"`
	Currency         string   `json:"currency"`
	Items            []string `json:"items"`
	SimulateFailStep string   `json:"simulate_fail_step,omitempty"`
}

// CheckoutResponse represents the response when a checkout workflow is accepted and processed.
type CheckoutResponse struct {
	WorkflowID    string                       `json:"workflow_id"`
	Status        string                       `json:"status"`
	Message       string                       `json:"message"`
	WorkflowState *eventsourcing.WorkflowState `json:"workflow_state,omitempty"`
	CreatedAt     time.Time                    `json:"created_at"`
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
	engine  *orchestrator.Engine
}

// NewHandler creates a new gateway Handler.
func NewHandler(l *limiter.TokenBucketLimiter) *Handler {
	return &Handler{
		limiter: l,
	}
}

// NewHandlerWithEngine creates a new gateway Handler wired to the Orchestrator Engine.
func NewHandlerWithEngine(l *limiter.TokenBucketLimiter, engine *orchestrator.Engine) *Handler {
	return &Handler{
		limiter: l,
		engine:  engine,
	}
}

// RegisterRoutes registers HTTP routes onto a standard http.ServeMux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", h.HealthCheck)
	mux.HandleFunc("POST /api/v1/workflows/checkout", h.HandleCheckout)
	mux.HandleFunc("GET /api/v1/workflows/{id}", h.HandleGetWorkflow)
}

// HealthCheck provides service health status.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// HandleCheckout validates the API key, checks rate limits, and dispatches checkout workflows to the Orchestrator.
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

	workflowID := "wf_" + uuid.NewString()

	// If Orchestrator Engine is wired, execute the Checkout Saga workflow
	var wfState *eventsourcing.WorkflowState
	status := "ACCEPTED"
	msg := "Checkout saga workflow accepted for execution"

	if h.engine != nil {
		wfState, err = h.engine.StartWorkflow(r.Context(), workflowID, req)
		if err != nil {
			log.Printf("[Gateway Error] Orchestrator failed for workflow %s: %v", workflowID, err)
			writeJSON(w, http.StatusInternalServerError, ErrorResponse{
				Error:   "Orchestration Error",
				Message: err.Error(),
			})
			return
		}
		status = string(wfState.Status)
		if wfState.Status == eventsourcing.WorkflowStatusRolledBack {
			msg = "Checkout saga failed and was rolled back via compensations"
		} else if wfState.Status == eventsourcing.WorkflowStatusCompleted {
			msg = "Checkout saga completed successfully"
		}
	}

	resp := CheckoutResponse{
		WorkflowID:    workflowID,
		Status:        status,
		Message:       msg,
		WorkflowState: wfState,
		CreatedAt:     time.Now().UTC(),
	}

	log.Printf("[Gateway] Processed workflow %s for customer %s -> Status: %s (Remaining tokens: %d)",
		workflowID, req.CustomerID, status, result.Remaining)

	writeJSON(w, http.StatusAccepted, resp)
}

// HandleGetWorkflow reconstructs and returns the state of a workflow by ID.
func (h *Handler) HandleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	if h.engine == nil {
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Error:   "Service Unavailable",
			Message: "Orchestrator Engine is not attached",
		})
		return
	}

	workflowID := r.PathValue("id")
	if workflowID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "Bad Request",
			Message: "Missing workflow ID",
		})
		return
	}

	state, err := h.engine.GetWorkflowState(r.Context(), workflowID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{
			Error:   "Not Found",
			Message: err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, state)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
