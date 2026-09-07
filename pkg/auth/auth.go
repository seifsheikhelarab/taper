// Package auth abstracts token issuance and verification behind ports so
// the gateway never depends on a specific identity provider. The sandbox
// implementation signs HS256 JWTs carrying a tenant_id claim; a real IdP
// adapter (RS256/JWKS) implements the same ports later.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Claims carried by every taper token. TenantID is the RLS identity the
// gateway injects into downstream requests.
type Claims struct {
	TenantID string `json:"tenant_id"`
	Expiry   int64  `json:"exp"` // unix seconds
}

// ErrUnauthenticated is returned for any token that fails verification:
// malformed, wrong signature, expired, or missing the tenant claim.
// Callers map it to HTTP 401 / gRPC unauthenticated.
var ErrUnauthenticated = errors.New("unauthenticated")

// TokenIssuer mints tokens for a tenant (sandbox/testing; a real IdP owns
// issuance in production).
type TokenIssuer interface {
	Issue(ctx context.Context, tenantID string, ttl time.Duration) (string, error)
}

// TokenVerifier validates a presented token and returns its claims.
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (Claims, error)
}

// Sandbox implements both ports with HMAC-SHA256 over a compact
// base64url(payload) token. Not an interoperable JWT wire format by
// design: it exists so integration tests and local runs have a working
// issuer, and so the ports are proven sufficient.
type Sandbox struct {
	secret []byte
	now    func() time.Time
}

// NewSandbox builds a sandbox issuer/verifier sharing secret.
func NewSandbox(secret []byte) *Sandbox {
	return &Sandbox{secret: secret, now: time.Now}
}

// Issue mints a token for tenantID valid for ttl.
func (s *Sandbox) Issue(_ context.Context, tenantID string, ttl time.Duration) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("%w: empty tenant", ErrUnauthenticated)
	}
	claims := Claims{TenantID: tenantID, Expiry: s.now().Add(ttl).Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(payload)
	return body + "." + s.sign(body), nil
}

// Verify validates the token signature and expiry, returning its claims.
func (s *Sandbox) Verify(_ context.Context, token string) (Claims, error) {
	body, sig, ok := strings.Cut(token, ".")
	if !ok || body == "" || sig == "" {
		return Claims{}, ErrUnauthenticated
	}
	want := s.sign(body)
	if len(sig) != len(want) || !hmac.Equal([]byte(sig), []byte(want)) {
		return Claims{}, ErrUnauthenticated
	}
	payload, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return Claims{}, ErrUnauthenticated
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return Claims{}, ErrUnauthenticated
	}
	if claims.TenantID == "" {
		return Claims{}, ErrUnauthenticated
	}
	if s.now().Unix() >= claims.Expiry {
		return Claims{}, ErrUnauthenticated
	}
	return claims, nil
}

func (s *Sandbox) sign(body string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(body))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
