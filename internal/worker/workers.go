package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/DaniloNogueira-Silva/go-distributed_worflow-orchestrator/internal/limiter"
	"github.com/google/uuid"
)

// CheckoutSagaPayload represents the payload passed across the Checkout Saga steps.
type CheckoutSagaPayload struct {
	CustomerID       string   `json:"customer_id"`
	Amount           float64  `json:"amount"`
	Currency         string   `json:"currency"`
	Items            []string `json:"items"`
	SimulateFailStep string   `json:"simulate_fail_step,omitempty"`
}

// StepHandler defines the behavior of a specific domain worker (Payment, Inventory, Fulfillment, Notification).
type StepHandler interface {
	Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error)
	Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error)
}

// PaymentWorker handles card charges and refunds, guarded by an Outbound Rate Limiter.
type PaymentWorker struct {
	outbound *limiter.OutboundLimiter
}

func NewPaymentWorker(outbound *limiter.OutboundLimiter) *PaymentWorker {
	return &PaymentWorker{outbound: outbound}
}

func (w *PaymentWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	// Check Outbound Rate Limiter before calling external Payment Gateway API
	delayed, err := w.outbound.Acquire(ctx, "payment_gateway")
	if err != nil {
		return nil, err
	}

	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Payment") {
		return nil, fmt.Errorf("payment gateway declined card for customer %s", data.CustomerID)
	}

	txID := "pay_" + uuid.NewString()[:8]
	log.Printf("[PaymentWorker] Charged %.2f %s for customer %s (tx=%s, outbound_delays=%d)",
		data.Amount, data.Currency, data.CustomerID, txID, delayed)

	return json.Marshal(map[string]interface{}{
		"transaction_id":  txID,
		"action":          action,
		"status":          "CHARGED",
		"outbound_delays": delayed,
		"processed_at":    time.Now().UTC(),
	})
}

func (w *PaymentWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	delayed, err := w.outbound.Acquire(ctx, "payment_gateway")
	if err != nil {
		return nil, err
	}

	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	refundID := "ref_" + uuid.NewString()[:8]
	log.Printf("[PaymentWorker] COMPENSATED (Refunded %.2f %s) for workflow %s (refund_id=%s, outbound_delays=%d)",
		data.Amount, data.Currency, workflowID, refundID, delayed)

	return json.Marshal(map[string]interface{}{
		"refund_id":       refundID,
		"action":          action,
		"status":          "REFUNDED",
		"outbound_delays": delayed,
		"compensated_at":  time.Now().UTC(),
	})
}

// InventoryWorker handles item reservations and releases.
type InventoryWorker struct{}

func NewInventoryWorker() *InventoryWorker {
	return &InventoryWorker{}
}

func (w *InventoryWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Inventory") {
		return nil, fmt.Errorf("insufficient stock for items %v", data.Items)
	}

	resID := "inv_" + uuid.NewString()[:8]
	log.Printf("[InventoryWorker] Reserved %d items for workflow %s (reservation_id=%s)",
		len(data.Items), workflowID, resID)

	return json.Marshal(map[string]interface{}{
		"reservation_id": resID,
		"action":         action,
		"items_reserved": data.Items,
		"status":         "RESERVED",
		"processed_at":   time.Now().UTC(),
	})
}

func (w *InventoryWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	log.Printf("[InventoryWorker] COMPENSATED (Released %d reserved items) for workflow %s",
		len(data.Items), workflowID)

	return json.Marshal(map[string]interface{}{
		"action":         action,
		"items_released": data.Items,
		"status":         "RELEASED",
		"compensated_at": time.Now().UTC(),
	})
}

// FulfillmentWorker generates and voids shipping labels, guarded by an Outbound Rate Limiter.
type FulfillmentWorker struct {
	outbound *limiter.OutboundLimiter
}

func NewFulfillmentWorker(outbound *limiter.OutboundLimiter) *FulfillmentWorker {
	return &FulfillmentWorker{outbound: outbound}
}

func (w *FulfillmentWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	// Check Outbound Rate Limiter before calling external Shipping Carrier API
	delayed, err := w.outbound.Acquire(ctx, "shipping_carrier")
	if err != nil {
		return nil, err
	}

	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Fulfillment") {
		return nil, fmt.Errorf("shipping carrier API error: unable to generate label for workflow %s", workflowID)
	}

	labelID := "lbl_" + uuid.NewString()[:8]
	log.Printf("[FulfillmentWorker] Generated shipping label %s for workflow %s (outbound_delays=%d)",
		labelID, workflowID, delayed)

	return json.Marshal(map[string]interface{}{
		"shipping_label_id": labelID,
		"action":            action,
		"status":            "LABEL_GENERATED",
		"outbound_delays":   delayed,
		"processed_at":      time.Now().UTC(),
	})
}

func (w *FulfillmentWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	delayed, err := w.outbound.Acquire(ctx, "shipping_carrier")
	if err != nil {
		return nil, err
	}

	log.Printf("[FulfillmentWorker] COMPENSATED (Voided shipping label) for workflow %s (outbound_delays=%d)",
		workflowID, delayed)

	return json.Marshal(map[string]interface{}{
		"action":          action,
		"status":          "LABEL_VOIDED",
		"outbound_delays": delayed,
		"compensated_at":  time.Now().UTC(),
	})
}

// NotificationWorker sends transactional emails to customers.
type NotificationWorker struct{}

func NewNotificationWorker() *NotificationWorker {
	return &NotificationWorker{}
}

func (w *NotificationWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Notification") {
		return nil, fmt.Errorf("SMTP server timeout while sending email to customer %s", data.CustomerID)
	}

	msgID := "msg_" + uuid.NewString()[:8]
	log.Printf("[NotificationWorker] Sent checkout confirmation email %s to customer %s",
		msgID, data.CustomerID)

	return json.Marshal(map[string]interface{}{
		"message_id":   msgID,
		"action":       action,
		"status":       "EMAIL_SENT",
		"processed_at": time.Now().UTC(),
	})
}

func (w *NotificationWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	// Non-critical step has no compensation
	return json.Marshal(map[string]interface{}{
		"status": "NO_OP",
	})
}
