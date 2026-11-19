package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultInitialCustomerBalance = 25000.00
	DefaultInitialProductStock    = 5000
)

// PaymentAccount represents a customer's wallet/credit account.
type PaymentAccount struct {
	CustomerID string    `json:"customer_id"`
	Balance    float64   `json:"balance"`
	Currency   string    `json:"currency"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// PaymentTransaction represents a financial entry in the payment ledger.
type PaymentTransaction struct {
	ID              string    `json:"transaction_id"`
	WorkflowID      string    `json:"workflow_id"`
	CustomerID      string    `json:"customer_id"`
	Amount          float64   `json:"amount"`
	Currency        string    `json:"currency"`
	Status          string    `json:"status"` // CHARGED | REFUNDED
	RefundID        string    `json:"refund_id,omitempty"`
	RemainingCredit float64   `json:"remaining_balance"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// InventoryProduct represents a product SKU in the warehouse catalog.
type InventoryProduct struct {
	SKU            string    `json:"sku"`
	Name           string    `json:"name"`
	AvailableStock int       `json:"available_stock"`
	ReservedStock  int       `json:"reserved_stock"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// InventoryReservation represents an active or released stock reservation for a checkout workflow.
type InventoryReservation struct {
	ID         string         `json:"reservation_id"`
	WorkflowID string         `json:"workflow_id"`
	Items      []string       `json:"items"`
	Status     string         `json:"status"` // RESERVED | RELEASED
	StockAfter map[string]int `json:"stock_after_operation,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// FulfillmentShipment represents a shipping label generated with a logistics carrier.
type FulfillmentShipment struct {
	ID           string    `json:"shipping_label_id"`
	WorkflowID   string    `json:"workflow_id"`
	CustomerID   string    `json:"customer_id"`
	TrackingCode string    `json:"tracking_code"`
	Carrier      string    `json:"carrier"`
	ItemCount    int       `json:"item_count"`
	Status       string    `json:"status"` // LABEL_GENERATED | VOIDED
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// NotificationMessage represents a transactional email stored in the notification outbox.
type NotificationMessage struct {
	ID             string    `json:"message_id"`
	WorkflowID     string    `json:"workflow_id"`
	CustomerID     string    `json:"customer_id"`
	RecipientEmail string    `json:"recipient_email"`
	Subject        string    `json:"subject"`
	Body           string    `json:"body"`
	Status         string    `json:"status"` // SENT
	CreatedAt      time.Time `json:"created_at"`
}

// DomainSnapshot aggregates current domain state across all 4 workers for inspection.
type DomainSnapshot struct {
	InventoryCatalog     []InventoryProduct   `json:"inventory_catalog"`
	TotalChargedCount    int                  `json:"total_charged_transactions"`
	TotalRefundedCount   int                  `json:"total_refunded_transactions"`
	NetCapturedRevenue   float64              `json:"net_captured_revenue"`
	TotalLabelsGenerated int                  `json:"total_labels_generated"`
	TotalLabelsVoided    int                  `json:"total_labels_voided"`
	TotalEmailsSent      int                  `json:"total_emails_sent"`
	RecentPayments       []PaymentTransaction `json:"recent_payments"`
	RecentShipments      []FulfillmentShipment `json:"recent_shipments"`
	RecentNotifications  []NotificationMessage `json:"recent_notifications"`
}

// DomainStore defines the persistence and business rules contract for the 4 domain workers.
type DomainStore interface {
	ChargeCustomer(ctx context.Context, workflowID, customerID string, amount float64, currency string) (*PaymentTransaction, error)
	RefundCustomer(ctx context.Context, workflowID string) (*PaymentTransaction, error)
	ReserveStock(ctx context.Context, workflowID string, items []string) (*InventoryReservation, error)
	ReleaseStock(ctx context.Context, workflowID string) (*InventoryReservation, error)
	CreateShipment(ctx context.Context, workflowID, customerID string, items []string) (*FulfillmentShipment, error)
	VoidShipment(ctx context.Context, workflowID string) (*FulfillmentShipment, error)
	SendNotification(ctx context.Context, workflowID, customerID string, amount float64, currency string, items []string) (*NotificationMessage, error)
	RestockProduct(ctx context.Context, sku string, quantity int) error
	SetCustomerBalance(ctx context.Context, customerID string, balance float64, currency string) error
	GetProductStock(ctx context.Context, sku string) (*InventoryProduct, error)
	GetCustomerAccount(ctx context.Context, customerID string) (*PaymentAccount, error)
	GetDomainSnapshot(ctx context.Context) (*DomainSnapshot, error)
}

func defaultCatalogSeed() map[string]int {
	return map[string]int{
		"keyboard_mech":       DefaultInitialProductStock,
		"mouse_wireless":      DefaultInitialProductStock,
		"monitor_4k":          DefaultInitialProductStock,
		"hdmi_cable":          DefaultInitialProductStock,
		"notebook_stand":      DefaultInitialProductStock,
		"mechanical_keyboard": DefaultInitialProductStock,
		"wrist_rest":          DefaultInitialProductStock,
		"ultrawide_monitor":   DefaultInitialProductStock,
		"go_programming_book": DefaultInitialProductStock,
		"headset":             DefaultInitialProductStock,
		"mousepad":            DefaultInitialProductStock,
		"gpu_rtx":             DefaultInitialProductStock,
		"psu_850w":            DefaultInitialProductStock,
		"item_alpha":          DefaultInitialProductStock,
		"item_beta":           DefaultInitialProductStock,
		"item_a":              DefaultInitialProductStock,
		"item_1":              DefaultInitialProductStock,
		"item-1":              DefaultInitialProductStock,
		"prod_1":              DefaultInitialProductStock,
		"livro_go":            DefaultInitialProductStock,
		"monitor_ultrawide":   DefaultInitialProductStock,
		"out_of_stock_item":   0, // Explicitly 0 to trigger real out-of-stock failures
	}
}

func initialBalanceForCustomer(customerID string) float64 {
	if strings.EqualFold(customerID, "broke_customer") {
		return 10.00
	}
	return DefaultInitialCustomerBalance
}

func countFrequencies(items []string) map[string]int {
	counts := make(map[string]int, len(items))
	for _, sku := range items {
		sku = strings.TrimSpace(sku)
		if sku != "" {
			counts[sku]++
		}
	}
	return counts
}

// ============================================================================
// IN-MEMORY DOMAIN STORE IMPLEMENTATION
// ============================================================================

type InMemoryDomainStore struct {
	mu            sync.RWMutex
	accounts      map[string]*PaymentAccount
	payments      map[string]*PaymentTransaction // keyed by workflowID
	paymentOrder  []string
	products      map[string]*InventoryProduct
	reservations  map[string]*InventoryReservation // keyed by workflowID
	shipments     map[string]*FulfillmentShipment  // keyed by workflowID
	shipmentOrder []string
	notifications []*NotificationMessage
}

func NewInMemoryDomainStore() *InMemoryDomainStore {
	s := &InMemoryDomainStore{
		accounts:     make(map[string]*PaymentAccount),
		payments:     make(map[string]*PaymentTransaction),
		products:     make(map[string]*InventoryProduct),
		reservations: make(map[string]*InventoryReservation),
		shipments:    make(map[string]*FulfillmentShipment),
	}
	now := time.Now().UTC()
	for sku, stock := range defaultCatalogSeed() {
		s.products[sku] = &InventoryProduct{
			SKU:            sku,
			Name:           strings.ReplaceAll(strings.ToUpper(sku), "_", " "),
			AvailableStock: stock,
			ReservedStock:  0,
			UpdatedAt:      now,
		}
	}
	return s
}

func (s *InMemoryDomainStore) ChargeCustomer(ctx context.Context, workflowID, customerID string, amount float64, currency string) (*PaymentTransaction, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("invalid payment amount %.2f", amount)
	}
	if currency == "" {
		currency = "BRL"
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.payments[workflowID]; ok {
		return existing, nil
	}

	acc, exists := s.accounts[customerID]
	if !exists {
		acc = &PaymentAccount{
			CustomerID: customerID,
			Balance:    initialBalanceForCustomer(customerID),
			Currency:   currency,
			UpdatedAt:  time.Now().UTC(),
		}
		s.accounts[customerID] = acc
	}

	if acc.Balance < amount {
		return nil, fmt.Errorf("insufficient funds for customer %s: balance %.2f %s < charge %.2f %s",
			customerID, acc.Balance, acc.Currency, amount, currency)
	}

	acc.Balance -= amount
	acc.UpdatedAt = time.Now().UTC()

	tx := &PaymentTransaction{
		ID:              "pay_" + uuid.NewString()[:8],
		WorkflowID:      workflowID,
		CustomerID:      customerID,
		Amount:          amount,
		Currency:        currency,
		Status:          "CHARGED",
		RemainingCredit: acc.Balance,
		CreatedAt:       acc.UpdatedAt,
		UpdatedAt:       acc.UpdatedAt,
	}
	s.payments[workflowID] = tx
	s.paymentOrder = append(s.paymentOrder, workflowID)
	return tx, nil
}

func (s *InMemoryDomainStore) RefundCustomer(ctx context.Context, workflowID string) (*PaymentTransaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, exists := s.payments[workflowID]
	if !exists {
		return nil, fmt.Errorf("payment transaction not found for workflow %s", workflowID)
	}
	if tx.Status == "REFUNDED" {
		return tx, nil
	}

	now := time.Now().UTC()
	if acc, ok := s.accounts[tx.CustomerID]; ok {
		acc.Balance += tx.Amount
		acc.UpdatedAt = now
		tx.RemainingCredit = acc.Balance
	}
	tx.Status = "REFUNDED"
	tx.RefundID = "ref_" + uuid.NewString()[:8]
	tx.UpdatedAt = now
	return tx, nil
}

func (s *InMemoryDomainStore) ReserveStock(ctx context.Context, workflowID string, items []string) (*InventoryReservation, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("cannot reserve empty item list for workflow %s", workflowID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.reservations[workflowID]; ok {
		return existing, nil
	}

	needed := countFrequencies(items)
	now := time.Now().UTC()

	// Validate availability across all requested SKUs before mutating any stock
	for sku, qty := range needed {
		prod, exists := s.products[sku]
		if !exists {
			initial := DefaultInitialProductStock
			if strings.EqualFold(sku, "out_of_stock_item") {
				initial = 0
			}
			prod = &InventoryProduct{
				SKU:            sku,
				Name:           strings.ReplaceAll(strings.ToUpper(sku), "_", " "),
				AvailableStock: initial,
				ReservedStock:  0,
				UpdatedAt:      now,
			}
			s.products[sku] = prod
		}
		if prod.AvailableStock < qty {
			return nil, fmt.Errorf("insufficient stock for SKU '%s': available=%d, requested=%d",
				sku, prod.AvailableStock, qty)
		}
	}

	// Deduct available stock and increment reserved stock atomically
	stockAfter := make(map[string]int, len(needed))
	for sku, qty := range needed {
		prod := s.products[sku]
		prod.AvailableStock -= qty
		prod.ReservedStock += qty
		prod.UpdatedAt = now
		stockAfter[sku] = prod.AvailableStock
	}

	res := &InventoryReservation{
		ID:         "inv_" + uuid.NewString()[:8],
		WorkflowID: workflowID,
		Items:      items,
		Status:     "RESERVED",
		StockAfter: stockAfter,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.reservations[workflowID] = res
	return res, nil
}

func (s *InMemoryDomainStore) ReleaseStock(ctx context.Context, workflowID string) (*InventoryReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	res, exists := s.reservations[workflowID]
	if !exists {
		return nil, fmt.Errorf("inventory reservation not found for workflow %s", workflowID)
	}
	if res.Status == "RELEASED" {
		return res, nil
	}

	now := time.Now().UTC()
	needed := countFrequencies(res.Items)
	stockAfter := make(map[string]int, len(needed))

	for sku, qty := range needed {
		if prod, ok := s.products[sku]; ok {
			prod.AvailableStock += qty
			if prod.ReservedStock >= qty {
				prod.ReservedStock -= qty
			} else {
				prod.ReservedStock = 0
			}
			prod.UpdatedAt = now
			stockAfter[sku] = prod.AvailableStock
		}
	}

	res.Status = "RELEASED"
	res.StockAfter = stockAfter
	res.UpdatedAt = now
	return res, nil
}

func (s *InMemoryDomainStore) CreateShipment(ctx context.Context, workflowID, customerID string, items []string) (*FulfillmentShipment, error) {
	for _, sku := range items {
		if strings.EqualFold(sku, "restricted_hazard_item") {
			return nil, fmt.Errorf("carrier rejected shipment: item '%s' is prohibited for air/ground parcel", sku)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.shipments[workflowID]; ok {
		return existing, nil
	}

	now := time.Now().UTC()
	uid := strings.ToUpper(uuid.NewString()[:8])
	sh := &FulfillmentShipment{
		ID:           "lbl_" + strings.ToLower(uid),
		WorkflowID:   workflowID,
		CustomerID:   customerID,
		TrackingCode: "TRK-BR-" + uid,
		Carrier:      "EXPRESS_LOGISTICS",
		ItemCount:    len(items),
		Status:       "LABEL_GENERATED",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	s.shipments[workflowID] = sh
	s.shipmentOrder = append(s.shipmentOrder, workflowID)
	return sh, nil
}

func (s *InMemoryDomainStore) VoidShipment(ctx context.Context, workflowID string) (*FulfillmentShipment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sh, exists := s.shipments[workflowID]
	if !exists {
		// Idempotent fallback if label record was never persisted
		now := time.Now().UTC()
		return &FulfillmentShipment{
			ID:         "lbl_void",
			WorkflowID: workflowID,
			Status:     "VOIDED",
			UpdatedAt:  now,
		}, nil
	}
	sh.Status = "VOIDED"
	sh.UpdatedAt = time.Now().UTC()
	return sh, nil
}

func (s *InMemoryDomainStore) SendNotification(ctx context.Context, workflowID, customerID string, amount float64, currency string, items []string) (*NotificationMessage, error) {
	if strings.EqualFold(customerID, "unsubscribed_customer") || strings.EqualFold(customerID, "invalid_email") {
		return nil, fmt.Errorf("SMTP delivery rejected for recipient %s@customer.local", customerID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	msg := &NotificationMessage{
		ID:             "msg_" + uuid.NewString()[:8],
		WorkflowID:     workflowID,
		CustomerID:     customerID,
		RecipientEmail: fmt.Sprintf("%s@customer.local", customerID),
		Subject:        fmt.Sprintf("Order Confirmation [%s]", workflowID),
		Body:           fmt.Sprintf("Payment of %.2f %s confirmed for %d item(s): %s.", amount, currency, len(items), strings.Join(items, ", ")),
		Status:         "SENT",
		CreatedAt:      now,
	}
	s.notifications = append(s.notifications, msg)
	return msg, nil
}

func (s *InMemoryDomainStore) RestockProduct(ctx context.Context, sku string, quantity int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	prod, exists := s.products[sku]
	if !exists {
		s.products[sku] = &InventoryProduct{
			SKU:            sku,
			Name:           strings.ReplaceAll(strings.ToUpper(sku), "_", " "),
			AvailableStock: quantity,
			ReservedStock:  0,
			UpdatedAt:      now,
		}
		return nil
	}
	prod.AvailableStock += quantity
	prod.UpdatedAt = now
	return nil
}

func (s *InMemoryDomainStore) SetCustomerBalance(ctx context.Context, customerID string, balance float64, currency string) error {
	if currency == "" {
		currency = "BRL"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[customerID] = &PaymentAccount{
		CustomerID: customerID,
		Balance:    balance,
		Currency:   currency,
		UpdatedAt:  time.Now().UTC(),
	}
	return nil
}

func (s *InMemoryDomainStore) GetProductStock(ctx context.Context, sku string) (*InventoryProduct, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prod, exists := s.products[sku]
	if !exists {
		return nil, fmt.Errorf("SKU %s not found", sku)
	}
	copied := *prod
	return &copied, nil
}

func (s *InMemoryDomainStore) GetCustomerAccount(ctx context.Context, customerID string) (*PaymentAccount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	acc, exists := s.accounts[customerID]
	if !exists {
		return &PaymentAccount{
			CustomerID: customerID,
			Balance:    initialBalanceForCustomer(customerID),
			Currency:   "BRL",
		}, nil
	}
	copied := *acc
	return &copied, nil
}

func (s *InMemoryDomainStore) GetDomainSnapshot(ctx context.Context) (*DomainSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := &DomainSnapshot{
		InventoryCatalog:    make([]InventoryProduct, 0, len(s.products)),
		RecentPayments:      make([]PaymentTransaction, 0),
		RecentShipments:     make([]FulfillmentShipment, 0),
		RecentNotifications: make([]NotificationMessage, 0),
	}

	for _, p := range s.products {
		snap.InventoryCatalog = append(snap.InventoryCatalog, *p)
	}
	sort.Slice(snap.InventoryCatalog, func(i, j int) bool {
		return snap.InventoryCatalog[i].SKU < snap.InventoryCatalog[j].SKU
	})

	for _, tx := range s.payments {
		if tx.Status == "CHARGED" {
			snap.TotalChargedCount++
			snap.NetCapturedRevenue += tx.Amount
		} else if tx.Status == "REFUNDED" {
			snap.TotalRefundedCount++
		}
	}

	for i := len(s.paymentOrder) - 1; i >= 0 && len(snap.RecentPayments) < 10; i-- {
		wfID := s.paymentOrder[i]
		if tx, ok := s.payments[wfID]; ok {
			snap.RecentPayments = append(snap.RecentPayments, *tx)
		}
	}

	for _, sh := range s.shipments {
		if sh.Status == "LABEL_GENERATED" {
			snap.TotalLabelsGenerated++
		} else if sh.Status == "VOIDED" {
			snap.TotalLabelsVoided++
		}
	}

	for i := len(s.shipmentOrder) - 1; i >= 0 && len(snap.RecentShipments) < 10; i-- {
		wfID := s.shipmentOrder[i]
		if sh, ok := s.shipments[wfID]; ok {
			snap.RecentShipments = append(snap.RecentShipments, *sh)
		}
	}

	snap.TotalEmailsSent = len(s.notifications)
	for i := len(s.notifications) - 1; i >= 0 && len(snap.RecentNotifications) < 10; i-- {
		snap.RecentNotifications = append(snap.RecentNotifications, *s.notifications[i])
	}

	return snap, nil
}

// ============================================================================
// POSTGRESQL DOMAIN STORE IMPLEMENTATION
// ============================================================================

type PostgresDomainStore struct {
	db *sql.DB
}

func NewPostgresDomainStore(db *sql.DB) *PostgresDomainStore {
	return &PostgresDomainStore{db: db}
}

func (p *PostgresDomainStore) EnsureSchemaAndSeed(ctx context.Context) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS payment_accounts (
		customer_id VARCHAR(64) PRIMARY KEY,
		balance NUMERIC(14, 2) NOT NULL DEFAULT 25000.00,
		currency VARCHAR(8) NOT NULL DEFAULT 'BRL',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS payment_transactions (
		id VARCHAR(64) PRIMARY KEY,
		workflow_id VARCHAR(64) NOT NULL UNIQUE,
		customer_id VARCHAR(64) NOT NULL,
		amount NUMERIC(14, 2) NOT NULL,
		currency VARCHAR(8) NOT NULL,
		status VARCHAR(32) NOT NULL,
		refund_id VARCHAR(64) NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_payment_tx_workflow_id ON payment_transactions(workflow_id);

	CREATE TABLE IF NOT EXISTS inventory_products (
		sku VARCHAR(64) PRIMARY KEY,
		name VARCHAR(128) NOT NULL,
		available_stock INT NOT NULL CHECK (available_stock >= 0),
		reserved_stock INT NOT NULL DEFAULT 0 CHECK (reserved_stock >= 0),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS inventory_reservations (
		id VARCHAR(64) PRIMARY KEY,
		workflow_id VARCHAR(64) NOT NULL UNIQUE,
		items JSONB NOT NULL DEFAULT '[]'::jsonb,
		status VARCHAR(32) NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_inventory_res_workflow_id ON inventory_reservations(workflow_id);

	CREATE TABLE IF NOT EXISTS fulfillment_shipments (
		id VARCHAR(64) PRIMARY KEY,
		workflow_id VARCHAR(64) NOT NULL UNIQUE,
		customer_id VARCHAR(64) NOT NULL,
		tracking_code VARCHAR(64) NOT NULL,
		carrier VARCHAR(64) NOT NULL DEFAULT 'EXPRESS_LOGISTICS',
		item_count INT NOT NULL,
		status VARCHAR(32) NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS notification_messages (
		id VARCHAR(64) PRIMARY KEY,
		workflow_id VARCHAR(64) NOT NULL,
		customer_id VARCHAR(64) NOT NULL,
		recipient_email VARCHAR(128) NOT NULL,
		subject VARCHAR(255) NOT NULL,
		body TEXT NOT NULL,
		status VARCHAR(32) NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`
	if _, err := p.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create domain tables: %w", err)
	}

	for sku, stock := range defaultCatalogSeed() {
		name := strings.ReplaceAll(strings.ToUpper(sku), "_", " ")
		_, err := p.db.ExecContext(ctx, `
			INSERT INTO inventory_products (sku, name, available_stock, reserved_stock, updated_at)
			VALUES ($1, $2, $3, 0, NOW())
			ON CONFLICT (sku) DO NOTHING
		`, sku, name, stock)
		if err != nil {
			return fmt.Errorf("seed inventory product %s: %w", sku, err)
		}
	}

	return nil
}

func (p *PostgresDomainStore) ChargeCustomer(ctx context.Context, workflowID, customerID string, amount float64, currency string) (*PaymentTransaction, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("invalid payment amount %.2f", amount)
	}
	if currency == "" {
		currency = "BRL"
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Ensure customer account exists
	initBal := initialBalanceForCustomer(customerID)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO payment_accounts (customer_id, balance, currency, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (customer_id) DO NOTHING
	`, customerID, initBal, currency)
	if err != nil {
		return nil, err
	}

	var balance float64
	err = tx.QueryRowContext(ctx, `
		SELECT balance FROM payment_accounts WHERE customer_id = $1 FOR UPDATE
	`, customerID).Scan(&balance)
	if err != nil {
		return nil, err
	}

	if balance < amount {
		return nil, fmt.Errorf("insufficient funds for customer %s: balance %.2f < charge %.2f", customerID, balance, amount)
	}

	newBalance := balance - amount
	_, err = tx.ExecContext(ctx, `
		UPDATE payment_accounts SET balance = $1, updated_at = NOW() WHERE customer_id = $2
	`, newBalance, customerID)
	if err != nil {
		return nil, err
	}

	txID := "pay_" + uuid.NewString()[:8]
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO payment_transactions (id, workflow_id, customer_id, amount, currency, status, refund_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, 'CHARGED', '', $6, $6)
	`, txID, workflowID, customerID, amount, currency, now)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &PaymentTransaction{
		ID:              txID,
		WorkflowID:      workflowID,
		CustomerID:      customerID,
		Amount:          amount,
		Currency:        currency,
		Status:          "CHARGED",
		RemainingCredit: newBalance,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func (p *PostgresDomainStore) RefundCustomer(ctx context.Context, workflowID string) (*PaymentTransaction, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var pt PaymentTransaction
	err = tx.QueryRowContext(ctx, `
		SELECT id, workflow_id, customer_id, amount, currency, status, refund_id, created_at
		FROM payment_transactions
		WHERE workflow_id = $1
		FOR UPDATE
	`, workflowID).Scan(&pt.ID, &pt.WorkflowID, &pt.CustomerID, &pt.Amount, &pt.Currency, &pt.Status, &pt.RefundID, &pt.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("lookup payment transaction for refund (%s): %w", workflowID, err)
	}

	if pt.Status == "REFUNDED" {
		_ = tx.Commit()
		return &pt, nil
	}

	var newBalance float64
	err = tx.QueryRowContext(ctx, `
		UPDATE payment_accounts
		SET balance = balance + $1, updated_at = NOW()
		WHERE customer_id = $2
		RETURNING balance
	`, pt.Amount, pt.CustomerID).Scan(&newBalance)
	if err != nil {
		return nil, err
	}

	refundID := "ref_" + uuid.NewString()[:8]
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		UPDATE payment_transactions
		SET status = 'REFUNDED', refund_id = $1, updated_at = $2
		WHERE workflow_id = $3
	`, refundID, now, workflowID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	pt.Status = "REFUNDED"
	pt.RefundID = refundID
	pt.RemainingCredit = newBalance
	pt.UpdatedAt = now
	return &pt, nil
}

func (p *PostgresDomainStore) ReserveStock(ctx context.Context, workflowID string, items []string) (*InventoryReservation, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("cannot reserve empty item list for workflow %s", workflowID)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	needed := countFrequencies(items)
	// Sort SKUs alphabetically before locking rows to prevent deadlocks across concurrent transactions
	skus := make([]string, 0, len(needed))
	for sku := range needed {
		skus = append(skus, sku)
	}
	sort.Strings(skus)

	stockAfter := make(map[string]int, len(skus))
	for _, sku := range skus {
		qty := needed[sku]
		initial := DefaultInitialProductStock
		if strings.EqualFold(sku, "out_of_stock_item") {
			initial = 0
		}
		name := strings.ReplaceAll(strings.ToUpper(sku), "_", " ")
		_, err = tx.ExecContext(ctx, `
			INSERT INTO inventory_products (sku, name, available_stock, reserved_stock, updated_at)
			VALUES ($1, $2, $3, 0, NOW())
			ON CONFLICT (sku) DO NOTHING
		`, sku, name, initial)
		if err != nil {
			return nil, err
		}

		var available int
		err = tx.QueryRowContext(ctx, `
			SELECT available_stock FROM inventory_products WHERE sku = $1 FOR UPDATE
		`, sku).Scan(&available)
		if err != nil {
			return nil, err
		}

		if available < qty {
			return nil, fmt.Errorf("insufficient stock for SKU '%s': available=%d, requested=%d", sku, available, qty)
		}

		newAvail := available - qty
		_, err = tx.ExecContext(ctx, `
			UPDATE inventory_products
			SET available_stock = available_stock - $1,
			    reserved_stock = reserved_stock + $1,
			    updated_at = NOW()
			WHERE sku = $2
		`, qty, sku)
		if err != nil {
			return nil, err
		}
		stockAfter[sku] = newAvail
	}

	resID := "inv_" + uuid.NewString()[:8]
	itemsJSON, _ := json.Marshal(items)
	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO inventory_reservations (id, workflow_id, items, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'RESERVED', $4, $4)
	`, resID, workflowID, itemsJSON, now)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &InventoryReservation{
		ID:         resID,
		WorkflowID: workflowID,
		Items:      items,
		Status:     "RESERVED",
		StockAfter: stockAfter,
		CreatedAt:  now,
		UpdatedAt:  now,
	}, nil
}

func (p *PostgresDomainStore) ReleaseStock(ctx context.Context, workflowID string) (*InventoryReservation, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var res InventoryReservation
	var itemsRaw []byte
	err = tx.QueryRowContext(ctx, `
		SELECT id, workflow_id, items, status, created_at
		FROM inventory_reservations
		WHERE workflow_id = $1
		FOR UPDATE
	`, workflowID).Scan(&res.ID, &res.WorkflowID, &itemsRaw, &res.Status, &res.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("lookup reservation for release (%s): %w", workflowID, err)
	}
	_ = json.Unmarshal(itemsRaw, &res.Items)

	if res.Status == "RELEASED" {
		_ = tx.Commit()
		return &res, nil
	}

	needed := countFrequencies(res.Items)
	skus := make([]string, 0, len(needed))
	for sku := range needed {
		skus = append(skus, sku)
	}
	sort.Strings(skus)

	stockAfter := make(map[string]int, len(skus))
	for _, sku := range skus {
		qty := needed[sku]
		var newAvail int
		err = tx.QueryRowContext(ctx, `
			UPDATE inventory_products
			SET available_stock = available_stock + $1,
			    reserved_stock = GREATEST(0, reserved_stock - $1),
			    updated_at = NOW()
			WHERE sku = $2
			RETURNING available_stock
		`, qty, sku).Scan(&newAvail)
		if err != nil {
			return nil, err
		}
		stockAfter[sku] = newAvail
	}

	now := time.Now().UTC()
	_, err = tx.ExecContext(ctx, `
		UPDATE inventory_reservations
		SET status = 'RELEASED', updated_at = $1
		WHERE workflow_id = $2
	`, now, workflowID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	res.Status = "RELEASED"
	res.StockAfter = stockAfter
	res.UpdatedAt = now
	return &res, nil
}

func (p *PostgresDomainStore) CreateShipment(ctx context.Context, workflowID, customerID string, items []string) (*FulfillmentShipment, error) {
	for _, sku := range items {
		if strings.EqualFold(sku, "restricted_hazard_item") {
			return nil, fmt.Errorf("carrier rejected shipment: item '%s' is prohibited for air/ground parcel", sku)
		}
	}

	now := time.Now().UTC()
	uid := strings.ToUpper(uuid.NewString()[:8])
	sh := &FulfillmentShipment{
		ID:           "lbl_" + strings.ToLower(uid),
		WorkflowID:   workflowID,
		CustomerID:   customerID,
		TrackingCode: "TRK-BR-" + uid,
		Carrier:      "EXPRESS_LOGISTICS",
		ItemCount:    len(items),
		Status:       "LABEL_GENERATED",
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	_, err := p.db.ExecContext(ctx, `
		INSERT INTO fulfillment_shipments (id, workflow_id, customer_id, tracking_code, carrier, item_count, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
	`, sh.ID, sh.WorkflowID, sh.CustomerID, sh.TrackingCode, sh.Carrier, sh.ItemCount, sh.Status, now)
	if err != nil {
		return nil, err
	}
	return sh, nil
}

func (p *PostgresDomainStore) VoidShipment(ctx context.Context, workflowID string) (*FulfillmentShipment, error) {
	now := time.Now().UTC()
	var sh FulfillmentShipment
	err := p.db.QueryRowContext(ctx, `
		UPDATE fulfillment_shipments
		SET status = 'VOIDED', updated_at = $1
		WHERE workflow_id = $2
		RETURNING id, workflow_id, customer_id, tracking_code, carrier, item_count, status, created_at, updated_at
	`, now, workflowID).Scan(
		&sh.ID, &sh.WorkflowID, &sh.CustomerID, &sh.TrackingCode, &sh.Carrier, &sh.ItemCount, &sh.Status, &sh.CreatedAt, &sh.UpdatedAt,
	)
	if err != nil {
		return &FulfillmentShipment{
			ID:         "lbl_void",
			WorkflowID: workflowID,
			Status:     "VOIDED",
			UpdatedAt:  now,
		}, nil
	}
	return &sh, nil
}

func (p *PostgresDomainStore) SendNotification(ctx context.Context, workflowID, customerID string, amount float64, currency string, items []string) (*NotificationMessage, error) {
	if strings.EqualFold(customerID, "unsubscribed_customer") || strings.EqualFold(customerID, "invalid_email") {
		return nil, fmt.Errorf("SMTP delivery rejected for recipient %s@customer.local", customerID)
	}

	now := time.Now().UTC()
	msg := &NotificationMessage{
		ID:             "msg_" + uuid.NewString()[:8],
		WorkflowID:     workflowID,
		CustomerID:     customerID,
		RecipientEmail: fmt.Sprintf("%s@customer.local", customerID),
		Subject:        fmt.Sprintf("Order Confirmation [%s]", workflowID),
		Body:           fmt.Sprintf("Payment of %.2f %s confirmed for %d item(s): %s.", amount, currency, len(items), strings.Join(items, ", ")),
		Status:         "SENT",
		CreatedAt:      now,
	}

	_, err := p.db.ExecContext(ctx, `
		INSERT INTO notification_messages (id, workflow_id, customer_id, recipient_email, subject, body, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, msg.ID, msg.WorkflowID, msg.CustomerID, msg.RecipientEmail, msg.Subject, msg.Body, msg.Status, now)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (p *PostgresDomainStore) RestockProduct(ctx context.Context, sku string, quantity int) error {
	name := strings.ReplaceAll(strings.ToUpper(sku), "_", " ")
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO inventory_products (sku, name, available_stock, reserved_stock, updated_at)
		VALUES ($1, $2, $3, 0, NOW())
		ON CONFLICT (sku) DO UPDATE
		SET available_stock = inventory_products.available_stock + EXCLUDED.available_stock,
		    updated_at = NOW()
	`, sku, name, quantity)
	return err
}

func (p *PostgresDomainStore) SetCustomerBalance(ctx context.Context, customerID string, balance float64, currency string) error {
	if currency == "" {
		currency = "BRL"
	}
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO payment_accounts (customer_id, balance, currency, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (customer_id) DO UPDATE
		SET balance = EXCLUDED.balance,
		    currency = EXCLUDED.currency,
		    updated_at = NOW()
	`, customerID, balance, currency)
	return err
}

func (p *PostgresDomainStore) GetProductStock(ctx context.Context, sku string) (*InventoryProduct, error) {
	var prod InventoryProduct
	err := p.db.QueryRowContext(ctx, `
		SELECT sku, name, available_stock, reserved_stock, updated_at
		FROM inventory_products WHERE sku = $1
	`, sku).Scan(&prod.SKU, &prod.Name, &prod.AvailableStock, &prod.ReservedStock, &prod.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &prod, nil
}

func (p *PostgresDomainStore) GetCustomerAccount(ctx context.Context, customerID string) (*PaymentAccount, error) {
	var acc PaymentAccount
	err := p.db.QueryRowContext(ctx, `
		SELECT customer_id, balance, currency, updated_at
		FROM payment_accounts WHERE customer_id = $1
	`, customerID).Scan(&acc.CustomerID, &acc.Balance, &acc.Currency, &acc.UpdatedAt)
	if err != nil {
		return &PaymentAccount{
			CustomerID: customerID,
			Balance:    initialBalanceForCustomer(customerID),
			Currency:   "BRL",
		}, nil
	}
	return &acc, nil
}

func (p *PostgresDomainStore) GetDomainSnapshot(ctx context.Context) (*DomainSnapshot, error) {
	snap := &DomainSnapshot{
		InventoryCatalog:    make([]InventoryProduct, 0),
		RecentPayments:      make([]PaymentTransaction, 0),
		RecentShipments:     make([]FulfillmentShipment, 0),
		RecentNotifications: make([]NotificationMessage, 0),
	}

	// 1. Inventory catalog
	rows, err := p.db.QueryContext(ctx, `
		SELECT sku, name, available_stock, reserved_stock, updated_at
		FROM inventory_products ORDER BY sku ASC
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var pr InventoryProduct
			if rows.Scan(&pr.SKU, &pr.Name, &pr.AvailableStock, &pr.ReservedStock, &pr.UpdatedAt) == nil {
				snap.InventoryCatalog = append(snap.InventoryCatalog, pr)
			}
		}
	}

	// 2. Payment ledger summary
	_ = p.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'CHARGED'),
			COUNT(*) FILTER (WHERE status = 'REFUNDED'),
			COALESCE(SUM(amount) FILTER (WHERE status = 'CHARGED'), 0)
		FROM payment_transactions
	`).Scan(&snap.TotalChargedCount, &snap.TotalRefundedCount, &snap.NetCapturedRevenue)

	pRows, err := p.db.QueryContext(ctx, `
		SELECT id, workflow_id, customer_id, amount, currency, status, refund_id, created_at, updated_at
		FROM payment_transactions ORDER BY created_at DESC LIMIT 10
	`)
	if err == nil {
		defer pRows.Close()
		for pRows.Next() {
			var pt PaymentTransaction
			if pRows.Scan(&pt.ID, &pt.WorkflowID, &pt.CustomerID, &pt.Amount, &pt.Currency, &pt.Status, &pt.RefundID, &pt.CreatedAt, &pt.UpdatedAt) == nil {
				snap.RecentPayments = append(snap.RecentPayments, pt)
			}
		}
	}

	// 3. Shipments summary
	_ = p.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'LABEL_GENERATED'),
			COUNT(*) FILTER (WHERE status = 'VOIDED')
		FROM fulfillment_shipments
	`).Scan(&snap.TotalLabelsGenerated, &snap.TotalLabelsVoided)

	sRows, err := p.db.QueryContext(ctx, `
		SELECT id, workflow_id, customer_id, tracking_code, carrier, item_count, status, created_at, updated_at
		FROM fulfillment_shipments ORDER BY created_at DESC LIMIT 10
	`)
	if err == nil {
		defer sRows.Close()
		for sRows.Next() {
			var sh FulfillmentShipment
			if sRows.Scan(&sh.ID, &sh.WorkflowID, &sh.CustomerID, &sh.TrackingCode, &sh.Carrier, &sh.ItemCount, &sh.Status, &sh.CreatedAt, &sh.UpdatedAt) == nil {
				snap.RecentShipments = append(snap.RecentShipments, sh)
			}
		}
	}

	// 4. Notifications summary
	_ = p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notification_messages`).Scan(&snap.TotalEmailsSent)

	nRows, err := p.db.QueryContext(ctx, `
		SELECT id, workflow_id, customer_id, recipient_email, subject, body, status, created_at
		FROM notification_messages ORDER BY created_at DESC LIMIT 10
	`)
	if err == nil {
		defer nRows.Close()
		for nRows.Next() {
			var nm NotificationMessage
			if nRows.Scan(&nm.ID, &nm.WorkflowID, &nm.CustomerID, &nm.RecipientEmail, &nm.Subject, &nm.Body, &nm.Status, &nm.CreatedAt) == nil {
				snap.RecentNotifications = append(snap.RecentNotifications, nm)
			}
		}
	}

	return snap, nil
}
