package payment

import (
	"context"
	"errors"
	"testing"
)

func TestSandboxChargeSuccess(t *testing.T) {
	g := NewSandbox()
	res, err := g.Charge(context.Background(), ChargeRequest{
		OrderID:  "ord_1",
		TenantID: "t1",
		Amount:   1000,
		Currency: "USD",
		IdemKey:  "k1",
	})
	if err != nil {
		t.Fatalf("expected success, got err: %v", err)
	}
	if res == nil || res.TransactionID == "" {
		t.Fatalf("expected transaction id, got %+v", res)
	}
}

func TestSandboxChargeForceFail(t *testing.T) {
	g := NewSandbox()
	_, err := g.Charge(context.Background(), ChargeRequest{
		OrderID:   "ord_2",
		Amount:    1000,
		Currency:  "USD",
		ForceFail: true,
	})
	if !errors.Is(err, ErrPaymentDeclined) {
		t.Fatalf("expected ErrPaymentDeclined, got %v", err)
	}
}

func TestSandboxChargeEnvFailList(t *testing.T) {
	t.Setenv("TAPER_SANDBOX_FAIL_ORDERS", "ord_a,ord_b")
	g := NewSandbox()
	_, err := g.Charge(context.Background(), ChargeRequest{
		OrderID: "ord_b",
		Amount:  500,
	})
	if !errors.Is(err, ErrPaymentDeclined) {
		t.Fatalf("expected decline for listed order, got %v", err)
	}
	res, err := g.Charge(context.Background(), ChargeRequest{
		OrderID: "ord_c",
		Amount:  500,
	})
	if err != nil {
		t.Fatalf("expected success for unlisted order, got %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestSandboxChargeInvalidAmount(t *testing.T) {
	g := NewSandbox()
	_, err := g.Charge(context.Background(), ChargeRequest{Amount: 0})
	if !errors.Is(err, ErrPaymentDeclined) {
		t.Fatalf("expected decline for zero amount, got %v", err)
	}
}
