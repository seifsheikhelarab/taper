package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seifsheikhelarab/taper/pkg/auth"
	"github.com/seifsheikhelarab/taper/pkg/ratelimit"
)

func TestParseAuthUsers(t *testing.T) {
	users, err := ParseAuthUsers("alice:pw1:tenant-a:read-write,bob:pw2:tenant-b:admin")
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 2 {
		t.Fatalf("users = %d, want 2", len(users))
	}
	if users[0] != (AuthUser{Username: "alice", Password: "pw1", TenantID: "tenant-a", Role: auth.RoleReadWrite}) {
		t.Fatalf("users[0] = %+v", users[0])
	}
	if users[1].TenantID != "tenant-b" || users[1].Role != auth.RoleAdmin {
		t.Fatalf("users[1] = %+v", users[1])
	}
}

func TestParseAuthUsersEmptyAndRejects(t *testing.T) {
	users, err := ParseAuthUsers("")
	if err != nil || users != nil {
		t.Fatalf("empty spec: users=%v err=%v, want none", users, err)
	}
	cases := []struct {
		name string
		spec string
	}{
		{"empty entry", "alice:pw1:tenant-a:read-write,,bob:pw2:tenant-b:admin"},
		{"missing field", "alice:pw1:tenant-a"},
		{"unknown role", "alice:pw1:tenant-a:superuser"},
		{"blank password", "alice::tenant-a:read-write"},
		{"duplicate username", "alice:pw1:tenant-a:read-write,alice:pw2:tenant-b:read-write"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseAuthUsers(tc.spec); err == nil {
				t.Fatalf("spec %q: expected error, got nil", tc.spec)
			}
		})
	}
}

func TestLoginHappyPath(t *testing.T) {
	c, verifier := newTestCore(t)
	users, err := ParseAuthUsers("alice:pw1:tenant-a:read-write")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewAuthRoutes(c, users, time.Minute).Mount(mux)
	h := c.Middleware(mux)

	rec, _ := doJSON(t, h, "POST", "/v1/auth/login", "", `{"username":"alice","password":"pw1"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("login: %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var out struct {
		Token     string `json:"token"`
		TokenType string `json:"token_type"`
		ExpiresIn int64  `json:"expires_in"`
		TenantID  string `json:"tenant_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Token == "" || out.TokenType != "Bearer" || out.ExpiresIn <= 0 || out.TenantID != "tenant-a" {
		t.Fatalf("response = %+v", out)
	}
	claims, err := verifier.Verify(context.Background(), out.Token)
	if err != nil {
		t.Fatalf("issued token does not verify: %v", err)
	}
	if claims.TenantID != "tenant-a" || claims.Subject != "alice" || claims.Role != auth.RoleReadWrite {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestLoginRejectsBadCredentials(t *testing.T) {
	c, _ := newTestCore(t)
	users, _ := ParseAuthUsers("alice:pw1:tenant-a:read-write")
	mux := http.NewServeMux()
	NewAuthRoutes(c, users, time.Minute).Mount(mux)
	h := c.Middleware(mux)

	cases := []struct {
		name string
		body string
	}{
		{"wrong password", `{"username":"alice","password":"nope"}`},
		{"unknown user", `{"username":"mallory","password":"pw1"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, _ := doJSON(t, h, "POST", "/v1/auth/login", "", tc.body, nil)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s: %d (%s), want 401", tc.name, rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "pw1") {
				t.Fatal("error response must not leak the configured password")
			}
		})
	}
}

func TestLoginMalformedBody(t *testing.T) {
	c, _ := newTestCore(t)
	users, _ := ParseAuthUsers("alice:pw1:tenant-a:read-write")
	mux := http.NewServeMux()
	NewAuthRoutes(c, users, time.Minute).Mount(mux)
	h := c.Middleware(mux)

	rec, _ := doJSON(t, h, "POST", "/v1/auth/login", "", `{not-json`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed json: %d, want 400", rec.Code)
	}
	rec, _ = doJSON(t, h, "POST", "/v1/auth/login", "", `{}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing credentials: %d, want 400", rec.Code)
	}
}

func TestLoginReachesWithoutBearerToken(t *testing.T) {
	c, _ := newTestCore(t)
	users, _ := ParseAuthUsers("alice:pw1:tenant-a:read-write")
	mux := http.NewServeMux()
	NewAuthRoutes(c, users, time.Minute).Mount(mux)
	h := c.Middleware(mux)

	rec, _ := doJSON(t, h, "POST", "/v1/auth/login", "", `{"username":"alice","password":"pw1"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("login without bearer token: %d (%s), want 200", rec.Code, rec.Body.String())
	}
}

func TestLoginRateLimited(t *testing.T) {
	c, _ := newTestCore(t)
	c.Limiter = ratelimit.New(1, 1) // 1/s, burst 1 — the login budget
	users, _ := ParseAuthUsers("alice:pw1:tenant-a:read-write")
	mux := http.NewServeMux()
	NewAuthRoutes(c, users, time.Minute).Mount(mux)
	h := c.Middleware(mux)

	rec, _ := doJSON(t, h, "POST", "/v1/auth/login", "", `{"username":"alice","password":"nope"}`, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("login attempt 1: %d, want 401", rec.Code)
	}
	rec, _ = doJSON(t, h, "POST", "/v1/auth/login", "", `{"username":"alice","password":"pw1"}`, nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("login attempt 2: %d, want 429 (login must ride the rate limit)", rec.Code)
	}
}
