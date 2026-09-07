package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSandboxRoundTrip(t *testing.T) {
	s := NewSandbox([]byte("test-secret"))
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
}

func TestSandboxRejectsBadTokens(t *testing.T) {
	s := NewSandbox([]byte("test-secret"))
	other := NewSandbox([]byte("other-secret"))
	tok, _ := s.Issue(context.Background(), "tenant-1", time.Minute)
	wrongSig, _ := other.Issue(context.Background(), "tenant-1", time.Minute)
	expired := func() string {
		past := s.now
		s.now = func() time.Time { return time.Now().Add(-2 * time.Minute) }
		defer func() { s.now = past }()
		tok, _ := s.Issue(context.Background(), "tenant-1", time.Minute)
		return tok
	}()

	cases := []struct {
		name  string
		token string
	}{
		{"garbage", "not-a-token"},
		{"empty", ""},
		{"missing signature", "abc"},
		{"wrong signature", wrongSig},
		{"expired", expired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.Verify(context.Background(), tc.token); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}
	_ = tok
}

func TestSandboxEmptyTenantRejected(t *testing.T) {
	s := NewSandbox([]byte("test-secret"))
	if _, err := s.Issue(context.Background(), "", time.Minute); err == nil {
		t.Fatal("empty tenant issue should fail")
	}
}
