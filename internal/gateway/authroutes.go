package gateway

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/seifsheikhelarab/taper/pkg/auth"
)

// AuthUser is one static credential pair plus the identity and role of the
// token its login issues.
type AuthUser struct {
	Username string
	Password string
	TenantID string
	Role     string
}

// ParseAuthUsers parses the GATEWAY_AUTH_USERS spec: a comma-separated list
// of username:password:tenant_id:role entries. Roles are validated against
// the auth package's known set; an empty spec yields no users (login then
// has nobody to authenticate). Duplicate usernames are a config error.
func ParseAuthUsers(spec string) ([]AuthUser, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	seen := make(map[string]bool)
	var users []AuthUser
	for i, entry := range strings.Split(spec, ",") {
		if strings.TrimSpace(entry) == "" {
			return nil, fmt.Errorf("GATEWAY_AUTH_USERS entry %d is empty", i+1)
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("GATEWAY_AUTH_USERS entry %d: want username:password:tenant_id:role, got %q", i+1, entry)
		}
		u := AuthUser{Username: parts[0], Password: parts[1], TenantID: parts[2], Role: parts[3]}
		switch u.Role {
		case auth.RoleReadOnly, auth.RoleReadWrite, auth.RoleAdmin:
		default:
			return nil, fmt.Errorf("GATEWAY_AUTH_USERS entry %d: unknown role %q", i+1, u.Role)
		}
		if u.Username == "" || u.Password == "" || u.TenantID == "" {
			return nil, fmt.Errorf("GATEWAY_AUTH_USERS entry %d: username, password and tenant_id are required", i+1)
		}
		if seen[u.Username] {
			return nil, fmt.Errorf("GATEWAY_AUTH_USERS entry %d: duplicate username %q", i+1, u.Username)
		}
		seen[u.Username] = true
		users = append(users, u)
	}
	return users, nil
}

// AuthRoutes serves POST /v1/auth/login: static-credential login that mints
// a gateway JWT. This is the sandbox stand-in for a real IdP; the issued
// token carries the identity and role configured for the user.
// ponytail: static in-memory credential list (plaintext passwords, no
// lockout, no per-user budget). Swap in an identity store + bcrypt + lockout
// when this graduates from a reference implementation to production.
type AuthRoutes struct {
	core  *Core
	users map[string]AuthUser
	ttl   time.Duration
}

// NewAuthRoutes builds the auth surface over a credential set and token TTL.
func NewAuthRoutes(c *Core, users []AuthUser, ttl time.Duration) *AuthRoutes {
	m := make(map[string]AuthUser, len(users))
	for _, u := range users {
		m[u.Username] = u
	}
	return &AuthRoutes{core: c, users: m, ttl: ttl}
}

// Mount registers the auth surface. The login route is exempted from bearer
// auth by Core.Middleware but still rides the per-source rate limit.
func (a *AuthRoutes) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST "+loginPath, a.login)
}

// login serves the static-credential token endpoint. Passwords are compared
// via SHA-256 + ConstantTimeCompare so response timing leaks neither the
// username's existence nor the password's length.
func (a *AuthRoutes) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		(&httpError{status: http.StatusBadRequest, code: "invalid_argument", message: "invalid JSON body"}).write(w)
		return
	}
	if req.Username == "" || req.Password == "" {
		(&httpError{status: http.StatusBadRequest, code: "invalid_argument", message: "username and password are required"}).write(w)
		return
	}
	u, ok := a.users[req.Username]
	reqHash := sha256.Sum256([]byte(req.Password))
	// Always hash the stored password so the branch on ok has no timing
	// side-channel. When !ok, u.Password is the zero string.
	storedHash := sha256.Sum256([]byte(u.Password))
	if !ok || subtle.ConstantTimeCompare(storedHash[:], reqHash[:]) != 1 {
		(&httpError{status: http.StatusUnauthorized, code: "unauthenticated", message: "invalid username or password"}).write(w)
		return
	}
	tok, err := a.core.Verifier.Issue(r.Context(), u.TenantID, a.ttl, auth.IssueOptions{Subject: u.Username, Role: u.Role})
	if err != nil {
		(&httpError{status: http.StatusInternalServerError, code: "internal", message: "token issue failed"}).write(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"token":      tok,
		"token_type": "Bearer",
		"expires_in": int(a.ttl.Seconds()),
		"tenant_id":  u.TenantID,
	})
}