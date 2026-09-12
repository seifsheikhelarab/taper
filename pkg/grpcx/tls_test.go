package grpcx

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInsecureFromEnv(t *testing.T) {
	cases := []struct {
		v    string
		want bool
	}{
		{"", false},         // default: never insecure
		{"0", false},        // explicit off
		{"1", true},         // explicit hatch
		{"true", true},      // explicit hatch
		{"yes", true},       // explicit hatch
		{"anything", false}, // unknown values are off
	}
	for _, tc := range cases {
		getenv := func(string) string { return tc.v }
		if got := InsecureFromEnv(getenv); got != tc.want {
			t.Errorf("GRPC_INSECURE=%q: got %v, want %v", tc.v, got, tc.want)
		}
	}
}

// ClientCredentials with no CA uses the system pool and requires nothing.
func TestClientCredentialsDefaultsToTLS(t *testing.T) {
	cfg, err := ClientCredentials(false, TLSClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("default posture must verify the server")
	}
	if cfg.MinVersion < tlsVersion12() {
		t.Fatal("TLS 1.2 is the floor")
	}
}

// The insecure hatch yields the zero config: grpcx maps it to
// insecure.NewCredentials() at the dial site (no TLS config applies).
func TestClientCredentialsInsecureHatch(t *testing.T) {
	cfg, err := ClientCredentials(true, TLSClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "" || len(cfg.Certificates) != 0 || cfg.RootCAs != nil {
		t.Fatal("insecure hatch must return an empty config for the plaintext dial")
	}
}

// A missing CA file is a boot-time failure, never a silent fallback.
func TestClientCredentialsBadCA(t *testing.T) {
	if _, err := ClientCredentials(false, TLSClientOptions{CAFile: "/nonexistent/ca.pem"}); !errors.Is(err, ErrTLSConfig) {
		t.Fatalf("err = %v, want ErrTLSConfig", err)
	}
}

// A real CA file loads and verifies.
func TestClientCredentialsCAFile(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, testCAPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ClientCredentials(false, TLSClientOptions{CAFile: caPath, ServerName: "stock.internal"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "stock.internal" {
		t.Fatalf("server name = %q", cfg.ServerName)
	}
	if len(cfg.RootCAs.Subjects()) == 0 { //nolint:staticcheck // Subjects is fine for an assertion
		t.Fatal("CA pool is empty")
	}
}

// ServerCredentials: plaintext when unset, error when half-configured,
// TLS when both files load.
func TestServerCredentials(t *testing.T) {
	if _, ok, err := ServerCredentials("", ""); err != nil || ok {
		t.Fatalf("unset: ok=%v err=%v, want false/nil", ok, err)
	}
	if _, ok, err := ServerCredentials("cert.pem", ""); !errors.Is(err, ErrTLSConfig) || ok {
		t.Fatalf("half-configured: ok=%v err=%v, want false/ErrTLSConfig", ok, err)
	}

	certPath, keyPath := writeServerCert(t)
	cfg, ok, err := ServerCredentials(certPath, keyPath)
	if err != nil || !ok {
		t.Fatalf("configured: ok=%v err=%v, want true/nil", ok, err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatal("server certificate not loaded")
	}
}

// --- helpers: throwaway CA and server cert for the tests ---

const tlsMin = 0 // placeholder to keep tlsVersion12 below readable

func tlsVersion12() uint16 { return 0x0303 }

func testCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "taper-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeServerCert(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, "server.pem")
	keyPath := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}
