package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestJWTRoundTrip(t *testing.T) {
	s := NewJWT([]byte("test-secret"))
	tok, err := s.Issue(context.Background(), "tenant-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := s.Verify(context.Background(), tok)
	if err != nil {
		t.Fatal(err)
	}
	if claims.TenantID != "tenant-1" {
		t.Fatalf("tenant = %q, want tenant-1", claims.TenantID)
	}
	if claims.Expiry <= time.Now().Unix() {
		t.Fatalf("exp = %d, want in the future", claims.Expiry)
	}
}

// The wire format is a standard three-segment HS256 JWT with the documented
// claims (spec #52: "claims I present are standard JWT-shaped").
func TestJWTWireFormat(t *testing.T) {
	s := NewJWT([]byte("test-secret"))
	tok, err := s.Issue(context.Background(), "tenant-1", time.Minute,
		IssueOptions{Role: RoleReadWrite, Subject: "user-7", Scopes: []string{OpOrderRead}})
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("segments = %d, want 3 (header.payload.signature)", len(parts))
	}
	var h jwtHeader
	if err := json.Unmarshal(mustB64(t, parts[0]), &h); err != nil {
		t.Fatal(err)
	}
	if h.Alg != "HS256" || h.Typ != "JWT" {
		t.Fatalf("header = %+v, want HS256/JWT", h)
	}
	var p map[string]any
	if err := json.Unmarshal(mustB64(t, parts[1]), &p); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tenant_id", "exp", "sub", "role", "scopes", "iat"} {
		if _, ok := p[key]; !ok {
			t.Fatalf("payload missing %q: %v", key, p)
		}
	}
	if p["role"] != RoleReadWrite || p["sub"] != "user-7" {
		t.Fatalf("role/sub = %v/%v", p["role"], p["sub"])
	}
}

func TestJWTRejectsBadTokens(t *testing.T) {
	s := NewJWT([]byte("test-secret"))
	other := NewJWT([]byte("other-secret"))
	wrongSig, _ := other.Issue(context.Background(), "tenant-1", time.Minute)
	expired := func() string {
		past := s.now
		s.now = func() time.Time { return time.Now().Add(-2 * time.Minute) }
		defer func() { s.now = past }()
		tok, _ := s.Issue(context.Background(), "tenant-1", time.Minute)
		return tok
	}()
	nbfFuture := func() string {
		// Craft a properly signed token whose nbf is an hour ahead.
		claims := Claims{
			TenantID:  "tenant-1",
			Expiry:    time.Now().Add(2 * time.Hour).Unix(),
			NotBefore: time.Now().Add(time.Hour).Unix(),
		}
		p, _ := json.Marshal(claims)
		h, _ := json.Marshal(jwtHeader{Alg: "HS256", Typ: "JWT"})
		body := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
		return body + "." + s.sign(body)
	}()

	cases := []struct {
		name  string
		token string
	}{
		{"garbage", "not-a-token"},
		{"empty", ""},
		{"missing signature", "abc"},
		{"missing payload", "abc.def"},
		{"wrong signature", wrongSig},
		{"expired", expired},
		{"not yet valid", nbfFuture},
		{"legacy sandbox format", "eyJ0ZW5hbnRfaWQiOiJ0ZW5hbnQtMSJ9.c2ln"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Verify(context.Background(), tc.token); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

// Tampering with the payload invalidates the signature (signature covers
// header.payload, not just the payload body).
func TestJWTTamperedPayloadRejected(t *testing.T) {
	s := NewJWT([]byte("test-secret"))
	tok, _ := s.Issue(context.Background(), "tenant-1", time.Minute)
	parts := strings.Split(tok, ".")
	parts[1] = swappedTenantPayload(t)
	if _, err := s.Verify(context.Background(), strings.Join(parts, ".")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("tampered payload err = %v, want ErrUnauthenticated", err)
	}
}

func TestJWTEmptyTenantRejected(t *testing.T) {
	s := NewJWT([]byte("test-secret"))
	if _, err := s.Issue(context.Background(), "", time.Minute); err == nil {
		t.Fatal("empty tenant issue should fail")
	}
}

// Role/scope enforcement (spec #52 US8/US9): a read-only token can read but
// mutation operations are rejected; explicit scopes narrow below the role;
// admin grants everything; unknown roles allow nothing.
func TestClaimsAllows(t *testing.T) {
	cases := []struct {
		name   string
		claims Claims
		op     string
		want   bool
	}{
		{"read-only can read", Claims{Role: RoleReadOnly}, OpOrderRead, true},
		{"read-only cannot mutate", Claims{Role: RoleReadOnly}, OpOrderMutate, false},
		{"read-write can mutate", Claims{Role: RoleReadWrite}, OpStockWrite, true},
		{"read-write cannot admin", Claims{Role: RoleReadWrite}, OpStockAdmin, false},
		{"admin everything", Claims{Role: RoleAdmin}, OpStockAdmin, true},
		{"unknown role nothing", Claims{Role: "superuser"}, OpOrderRead, false},
		{"legacy empty role reads", Claims{}, OpOrderRead, true},
		{"legacy empty role cannot admin", Claims{}, OpStockAdmin, false},
		{"scopes narrow grant", Claims{Role: RoleReadWrite, Scopes: []string{OpOrderRead}}, OpStockWrite, false},
		{"scopes grant in role set", Claims{Role: RoleReadWrite, Scopes: []string{OpOrderRead}}, OpOrderRead, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.claims.Allows(tc.op); got != tc.want {
				t.Fatalf("Allows(%q) = %v, want %v", tc.op, got, tc.want)
			}
		})
	}
}

func swappedTenantPayload(t *testing.T) string {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"tenant_id": "tenant-2", "exp": time.Now().Add(time.Hour).Unix()})
	return base64.RawURLEncoding.EncodeToString(b)
}

func mustB64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
