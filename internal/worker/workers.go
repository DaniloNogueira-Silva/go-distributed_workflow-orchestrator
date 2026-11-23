package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/limiter"
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

// PaymentWorker handles real customer account balance deductions and ledger refunds, guarded by an Outbound Rate Limiter.
type PaymentWorker struct {
	outbound *limiter.OutboundLimiter
	store    DomainStore
}

func NewPaymentWorker(outbound *limiter.OutboundLimiter, store DomainStore) *PaymentWorker {
	if store == nil {
		store = NewInMemoryDomainStore()
	}
	return &PaymentWorker{outbound: outbound, store: store}
}

func (w *PaymentWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	delayed, err := w.outbound.Acquire(ctx, "payment_gateway")
	if err != nil {
		return nil, err
	}
	delayMeta, _ := json.Marshal(map[string]interface{}{"outbound_delays": delayed})

	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Payment") {
		return delayMeta, fmt.Errorf("payment gateway declined card for customer %s", data.CustomerID)
	}

	tx, err := w.store.ChargeCustomer(ctx, workflowID, data.CustomerID, data.Amount, data.Currency)
	if err != nil {
		return delayMeta, err
	}

	log.Printf("[PaymentWorker] Charged %.2f %s from customer %s (tx=%s, remaining_balance=%.2f, outbound_delays=%d)",
		tx.Amount, tx.Currency, tx.CustomerID, tx.ID, tx.RemainingCredit, delayed)

	return json.Marshal(map[string]interface{}{
		"transaction_id":    tx.ID,
		"action":            action,
		"status":            tx.Status,
		"amount":            tx.Amount,
		"currency":          tx.Currency,
		"remaining_balance": tx.RemainingCredit,
		"outbound_delays":   delayed,
		"processed_at":      tx.CreatedAt,
	})
}

func (w *PaymentWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	delayed, err := w.outbound.Acquire(ctx, "payment_gateway")
	if err != nil {
		return nil, err
	}
	delayMeta, _ := json.Marshal(map[string]interface{}{"outbound_delays": delayed})

	tx, err := w.store.RefundCustomer(ctx, workflowID)
	if err != nil {
		return delayMeta, err
	}

	log.Printf("[PaymentWorker] COMPENSATED (Refunded %.2f %s to customer %s) for workflow %s (refund_id=%s, restored_balance=%.2f, outbound_delays=%d)",
		tx.Amount, tx.Currency, tx.CustomerID, workflowID, tx.RefundID, tx.RemainingCredit, delayed)

	return json.Marshal(map[string]interface{}{
		"transaction_id":   tx.ID,
		"refund_id":        tx.RefundID,
		"action":           action,
		"status":           tx.Status,
		"amount_refunded":  tx.Amount,
		"restored_balance": tx.RemainingCredit,
		"outbound_delays":  delayed,
		"compensated_at":   tx.UpdatedAt,
	})
}

// InventoryWorker handles catalog stock reservations and stock restoration on rollback.
type InventoryWorker struct {
	store DomainStore
}

func NewInventoryWorker(store DomainStore) *InventoryWorker {
	if store == nil {
		store = NewInMemoryDomainStore()
	}
	return &InventoryWorker{store: store}
}

func (w *InventoryWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Inventory") {
		return nil, fmt.Errorf("insufficient stock for items %v", data.Items)
	}

	res, err := w.store.ReserveStock(ctx, workflowID, data.Items)
	if err != nil {
		return nil, err
	}

	log.Printf("[InventoryWorker] Reserved %d item(s) %v for workflow %s (reservation_id=%s, stock_after=%v)",
		len(res.Items), res.Items, workflowID, res.ID, res.StockAfter)

	return json.Marshal(map[string]interface{}{
		"reservation_id":        res.ID,
		"action":                action,
		"items_reserved":        res.Items,
		"stock_after_operation": res.StockAfter,
		"status":                res.Status,
		"processed_at":          res.CreatedAt,
	})
}

func (w *InventoryWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	res, err := w.store.ReleaseStock(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	log.Printf("[InventoryWorker] COMPENSATED (Restored %d item(s) %v to available stock) for workflow %s (stock_after=%v)",
		len(res.Items), res.Items, workflowID, res.StockAfter)

	return json.Marshal(map[string]interface{}{
		"reservation_id":        res.ID,
		"action":                action,
		"items_released":        res.Items,
		"stock_after_operation": res.StockAfter,
		"status":                res.Status,
		"compensated_at":        res.UpdatedAt,
	})
}

// FulfillmentWorker persists shipping labels and voids them on rollback, guarded by an Outbound Rate Limiter.
type FulfillmentWorker struct {
	outbound *limiter.OutboundLimiter
	store    DomainStore
}

func NewFulfillmentWorker(outbound *limiter.OutboundLimiter, store DomainStore) *FulfillmentWorker {
	if store == nil {
		store = NewInMemoryDomainStore()
	}
	return &FulfillmentWorker{outbound: outbound, store: store}
}

func (w *FulfillmentWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	delayed, err := w.outbound.Acquire(ctx, "shipping_carrier")
	if err != nil {
		return nil, err
	}
	delayMeta, _ := json.Marshal(map[string]interface{}{"outbound_delays": delayed})

	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Fulfillment") {
		return delayMeta, fmt.Errorf("shipping carrier API error: unable to generate label for workflow %s", workflowID)
	}

	sh, err := w.store.CreateShipment(ctx, workflowID, data.CustomerID, data.Items)
	if err != nil {
		return delayMeta, err
	}

	log.Printf("[FulfillmentWorker] Generated shipping label %s (tracking=%s) for workflow %s (outbound_delays=%d)",
		sh.ID, sh.TrackingCode, workflowID, delayed)

	return json.Marshal(map[string]interface{}{
		"shipping_label_id": sh.ID,
		"tracking_code":     sh.TrackingCode,
		"carrier":           sh.Carrier,
		"action":            action,
		"status":            sh.Status,
		"outbound_delays":   delayed,
		"processed_at":      sh.CreatedAt,
	})
}

func (w *FulfillmentWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	delayed, err := w.outbound.Acquire(ctx, "shipping_carrier")
	if err != nil {
		return nil, err
	}
	delayMeta, _ := json.Marshal(map[string]interface{}{"outbound_delays": delayed})

	sh, err := w.store.VoidShipment(ctx, workflowID)
	if err != nil {
		return delayMeta, err
	}

	log.Printf("[FulfillmentWorker] COMPENSATED (Voided shipping label %s) for workflow %s (outbound_delays=%d)",
		sh.ID, workflowID, delayed)

	return json.Marshal(map[string]interface{}{
		"shipping_label_id": sh.ID,
		"tracking_code":     sh.TrackingCode,
		"action":            action,
		"status":            sh.Status,
		"outbound_delays":   delayed,
		"compensated_at":    sh.UpdatedAt,
	})
}

// NotificationWorker formats and persists transactional emails to the notification outbox.
type NotificationWorker struct {
	store DomainStore
}

func NewNotificationWorker(store DomainStore) *NotificationWorker {
	if store == nil {
		store = NewInMemoryDomainStore()
	}
	return &NotificationWorker{store: store}
}

func (w *NotificationWorker) Execute(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	var data CheckoutSagaPayload
	_ = json.Unmarshal(payload, &data)

	if strings.EqualFold(data.SimulateFailStep, "Notification") {
		return nil, fmt.Errorf("SMTP server timeout while sending email to customer %s", data.CustomerID)
	}

	msg, err := w.store.SendNotification(ctx, workflowID, data.CustomerID, data.Amount, data.Currency, data.Items)
	if err != nil {
		return nil, err
	}

	log.Printf("[NotificationWorker] Sent & persisted confirmation email %s to %s (%s)",
		msg.ID, msg.RecipientEmail, msg.Subject)

	return json.Marshal(map[string]interface{}{
		"message_id":      msg.ID,
		"recipient_email": msg.RecipientEmail,
		"subject":         msg.Subject,
		"action":          action,
		"status":          msg.Status,
		"processed_at":    msg.CreatedAt,
	})
}

func (w *NotificationWorker) Compensate(ctx context.Context, workflowID, action string, payload []byte) ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"status": "NO_OP",
	})
}
