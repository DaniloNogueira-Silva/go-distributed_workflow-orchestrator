package worker_test

import (
	"context"
	"testing"

	"github.com/DaniloNogueira-Silva/go-distributed_workflow-orchestrator/internal/worker"
)

func TestDomainStore_PaymentChargeAndRefundRestoresBalance(t *testing.T) {
	ctx := context.Background()
	store := worker.NewInMemoryDomainStore()

	_ = store.SetCustomerBalance(ctx, "cust_fin_1", 1000.00, "BRL")

	tx, err := store.ChargeCustomer(ctx, "wf_pay_1", "cust_fin_1", 350.00, "BRL")
	if err != nil {
		t.Fatalf("unexpected charge error: %v", err)
	}
	if tx.RemainingCredit != 650.00 {
		t.Fatalf("expected remaining balance 650.00, got %.2f", tx.RemainingCredit)
	}

	// Attempt a charge exceeding remaining balance
	_, err = store.ChargeCustomer(ctx, "wf_pay_2", "cust_fin_1", 700.00, "BRL")
	if err == nil {
		t.Fatalf("expected insufficient funds error when charging 700.00 against 650.00 balance")
	}

	// Refund the first workflow and verify balance returns to 1000.00
	refTx, err := store.RefundCustomer(ctx, "wf_pay_1")
	if err != nil {
		t.Fatalf("unexpected refund error: %v", err)
	}
	if refTx.Status != "REFUNDED" || refTx.RemainingCredit != 1000.00 {
		t.Fatalf("expected REFUNDED with restored balance 1000.00, got status=%s balance=%.2f", refTx.Status, refTx.RemainingCredit)
	}
}

func TestDomainStore_InventoryReserveReleaseAndAtomicOutOfStock(t *testing.T) {
	ctx := context.Background()
	store := worker.NewInMemoryDomainStore()

	before, _ := store.GetProductStock(ctx, "gpu_rtx")
	initialStock := before.AvailableStock

	res, err := store.ReserveStock(ctx, "wf_inv_1", []string{"gpu_rtx", "gpu_rtx"})
	if err != nil {
		t.Fatalf("unexpected reserve error: %v", err)
	}

	afterReserve, _ := store.GetProductStock(ctx, "gpu_rtx")
	if afterReserve.AvailableStock != initialStock-2 || afterReserve.ReservedStock != 2 {
		t.Fatalf("expected available=%d reserved=2, got available=%d reserved=%d",
			initialStock-2, afterReserve.AvailableStock, afterReserve.ReservedStock)
	}

	// Atomic check: reserving a valid item junto com out_of_stock_item must not deduct gpu_rtx
	_, err = store.ReserveStock(ctx, "wf_inv_fail", []string{"gpu_rtx", "out_of_stock_item"})
	if err == nil {
		t.Fatalf("expected error when reserving out_of_stock_item")
	}
	afterFailed, _ := store.GetProductStock(ctx, "gpu_rtx")
	if afterFailed.AvailableStock != initialStock-2 {
		t.Fatalf("expected atomic rollback on failed multi-item reservation, got available=%d", afterFailed.AvailableStock)
	}

	// Release reservation and verify stock returns to initialStock
	rel, err := store.ReleaseStock(ctx, res.WorkflowID)
	if err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}
	if rel.Status != "RELEASED" {
		t.Fatalf("expected status RELEASED, got %s", rel.Status)
	}

	afterRelease, _ := store.GetProductStock(ctx, "gpu_rtx")
	if afterRelease.AvailableStock != initialStock || afterRelease.ReservedStock != 0 {
		t.Fatalf("expected stock restored to available=%d reserved=0, got available=%d reserved=%d",
			initialStock, afterRelease.AvailableStock, afterRelease.ReservedStock)
	}
}
