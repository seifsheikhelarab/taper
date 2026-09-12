// Package auth implements the gateway's bearer-token issuer/verifier
// (spec #52, B2 Security): standards-shaped HS256 JWTs carrying subject +
// tenant_id + role + scopes + exp, verified with constant-time HMAC.
// Roles map to allowed-operation sets enforced at the gateway before
// dispatch, so a leaked token's blast radius is bounded (least privilege at
// the surface).
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrUnauthenticated is returned for any token that fails verification:
// malformed, wrong signature, expired, or missing the tenant claim.
// Callers map it to HTTP 401 / gRPC unauthenticated.
var ErrUnauthenticated = errors.New("unauthenticated")

// JWT is an HS256 token in the standard three-segment wire form:
// base64url(header).base64url(payload).base64url(signature).
type JWT struct {
	secret []byte
	now    func() time.Time
}

// Roles carried by the role claim. RoleALLOperations is the mint-side
// wildcard for internal harnesses and admin tooling; the gateway enforces
// the role -> operation mapping on every request regardless.
const (
	RoleReadOnly  = "read-only"
	RoleReadWrite = "read-write"
	RoleAdmin     = "admin"
)

// Operations are the gateway's dispatch surface, declared per route.
const (
	OpStockRead         = "stock:read"
	OpStockWrite        = "stock:write"
	OpStockAdmin        = "stock:admin"
	OpReservationRead   = "reservation:read"
	OpReservationMutate = "reservation:mutate"
	OpOrderRead         = "order:read"
	OpOrderMutate       = "order:mutate"
)

// Claims carried by every taper token. TenantID is the RLS identity the
// gateway injects into downstream requests; Role bounds the allowed
// operations; Scopes optionally narrows below the role (empty = full role
// grant).
type Claims struct {
	TenantID  string   `json:"tenant_id"`
	Expiry    int64    `json:"exp"` // unix seconds
	Subject   string   `json:"sub,omitempty"`
	Role      string   `json:"role,omitempty"`
	Scopes    []string `json:"scopes,omitempty"`
	IssuedAt  int64    `json:"iat,omitempty"`
	NotBefore int64    `json:"nbf,omitempty"`
}

// jwtHeader is fixed for this implementation.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// NewJWT builds an HS256 issuer/verifier sharing secret.
func NewJWT(secret []byte) *JWT {
	return &JWT{secret: secret, now: time.Now}
}

// NewSandbox is a backwards-compatible alias for NewJWT. The sandbox wire
// format is gone: tokens are standard HS256 JWTs now. Kept so existing
// call sites and tests read naturally while the format migrates.
func NewSandbox(secret []byte) *JWT { return NewJWT(secret) }

// IssueOptions tunes Issue beyond the required tenant. TTL stays positional
// on Issue (the preserved surface shape); only identity claims ride here.
type IssueOptions struct {
	Subject string
	Role    string
	Scopes  []string
}

// Issue mints an HS256 JWT for tenantID valid for ttl (default 15m).
func (j *JWT) Issue(_ context.Context, tenantID string, ttl time.Duration, opts ...IssueOptions) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("%w: empty tenant", ErrUnauthenticated)
	}
	var o IssueOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	now := j.now()
	claims := Claims{
		TenantID: tenantID,
		Expiry:   now.Add(ttl).Unix(),
		Subject:  o.Subject,
		Role:     o.Role,
		Scopes:   o.Scopes,
		IssuedAt: now.Unix(),
	}
	h, err := json.Marshal(jwtHeader{Alg: "HS256", Typ: "JWT"})
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	return body + "." + j.sign(body), nil
}

// Verify validates the token signature (constant-time), expiry, and nbf,
// returning its claims. The tenant claim is mandatory.
func (j *JWT) Verify(_ context.Context, token string) (Claims, error) {
	header, rest, ok := strings.Cut(token, ".")
	if !ok {
		return Claims{}, ErrUnauthenticated
	}
	payload, sig, ok := strings.Cut(rest, ".")
	if !ok || header == "" || payload == "" || sig == "" {
		return Claims{}, ErrUnauthenticated
	}
	want := j.sign(header + "." + payload)
	if len(sig) != len(want) ||
		subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return Claims{}, ErrUnauthenticated
	}
	hb, err := base64.RawURLEncoding.DecodeString(header)
	if err != nil {
		return Claims{}, ErrUnauthenticated
	}
	var h jwtHeader
	if err := json.Unmarshal(hb, &h); err != nil || h.Alg != "HS256" {
		return Claims{}, ErrUnauthenticated
	}
	pb, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return Claims{}, ErrUnauthenticated
	}
	var claims Claims
	if err := json.Unmarshal(pb, &claims); err != nil {
		return Claims{}, ErrUnauthenticated
	}
	if claims.TenantID == "" {
		return Claims{}, ErrUnauthenticated
	}
	now := j.now().Unix()
	if now >= claims.Expiry {
		return Claims{}, ErrUnauthenticated
	}
	if claims.NotBefore != 0 && now < claims.NotBefore {
		return Claims{}, ErrUnauthenticated
	}
	return claims, nil
}

// Allows reports whether the claims cover op. Admin grants everything;
// otherwise explicit scopes, when present, are the grant (role is then just
// the default); with no scopes the role's full set applies. Tokens with an
// unknown role allow nothing; a missing role is the legacy grant
// (read-write) so tokens minted before the role claim existed keep working.
func (c Claims) Allows(op string) bool {
	switch c.Role {
	case RoleAdmin:
		return true
	case RoleReadOnly, RoleReadWrite, "": // fall through to scope/role-set logic
	default:
		return false
	}
	role := c.Role
	if role == "" {
		role = RoleReadWrite
	}
	if len(c.Scopes) > 0 {
		for _, s := range c.Scopes {
			if s == op {
				return true
			}
		}
		return false
	}
	for _, o := range roleOperations[role] {
		if o == op {
			return true
		}
	}
	return false
}

// roleOperations is the role -> allowed-operation mapping enforced at the
// gateway before dispatch. Admin is handled inline (allows everything).
var roleOperations = map[string][]string{
	RoleReadOnly: {
		OpStockRead, OpReservationRead, OpOrderRead,
	},
	RoleReadWrite: {
		OpStockRead, OpStockWrite,
		OpReservationRead, OpReservationMutate,
		OpOrderRead, OpOrderMutate,
	},
}

// sign computes the base64url HMAC-SHA256 over the signing input.
func (j *JWT) sign(input string) string {
	mac := hmac.New(sha256.New, j.secret)
	mac.Write([]byte(input))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
