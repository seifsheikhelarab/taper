// Package payment abstracts the payment gateway behind an interface so the
// order saga can drive payments without coupling to a specific provider.
package payment

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
)

// ErrPaymentDeclined is returned when the gateway refuses the charge.
var ErrPaymentDeclined = errors.New("payment declined")

// ChargeRequest describes a one-time authorization against the gateway.
type ChargeRequest struct {
	OrderID   string
	TenantID  string
	Amount    int64  // minor units (e.g. cents)
	Currency  string // ISO 4217, e.g. "USD"
	IdemKey   string // idempotency key for the gateway call
	ForceFail bool   // test hook: force a decline (sandbox only)
}

// ChargeResult is the outcome of a successful authorization.
type ChargeResult struct {
	TransactionID string
	AuthorizedAt  time.Time
}

// Gateway is the payment provider boundary used by the order saga.
type Gateway interface {
	// Charge authorizes (and captures, for sandbox simplicity) a payment.
	// Implementations must be idempotent for a given IdemKey.
	Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error)
}

// Sandbox is a deterministic in-process gateway for local dev and tests.
// It succeeds unless the request opts into failure via ForceFail, or the
// TAPER_SANDBOX_FAIL_ORDERS env var lists the order id.
type Sandbox struct{}

// NewSandbox returns a Sandbox gateway.
func NewSandbox() *Sandbox { return &Sandbox{} }

// Charge implements Gateway with deterministic sandbox behavior.
func (s *Sandbox) Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error) {
	if req.Amount <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", ErrPaymentDeclined)
	}
	if req.ForceFail || sandboxFailListed(req.OrderID) {
		return nil, fmt.Errorf("%w: order %s", ErrPaymentDeclined, req.OrderID)
	}
	return &ChargeResult{
		TransactionID: "sbx_" + uuid.NewString(),
		AuthorizedAt:  time.Now().UTC(),
	}, nil
}

// sandboxFailListed checks TAPER_SANDBOX_FAIL_ORDERS (comma-separated order
// ids) so integration tests can force declines via configuration.
func sandboxFailListed(orderID string) bool {
	v := os.Getenv("TAPER_SANDBOX_FAIL_ORDERS")
	if v == "" {
		return false
	}
	for _, id := range splitComma(v) {
		if id == orderID {
			return true
		}
	}
	return false
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
