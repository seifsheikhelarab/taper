// TLS posture for the repo's gRPC dials and listeners (spec #52, B2 /
// US10-11): all internal dials move to TLS-capable credentials governed by
// a GRPC_TLS_* env surface, with plaintext reserved for an explicit
// GRPC_INSECURE=1 escape hatch in local dev. Mutual TLS at a service-mesh
// layer is out of scope; plain TLS on the dials is the deliverable.
package grpcx

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ErrTLSConfig describes a loadable-certificate problem at boot.
var ErrTLSConfig = errors.New("grpcx: tls configuration error")

// TLSClientOptions describe the dial-side TLS material.
type TLSClientOptions struct {
	// CAFile is the CA bundle verifying the server certificate. Empty
	// means the system pool.
	CAFile string
	// CertFile/KeyFile optionally enable mutual TLS from the dial side.
	CertFile string
	KeyFile  string
	// ServerName overrides the certificate verification name (useful when
	// dialing by IP or through a port-forward).
	ServerName string
}

// TLSFromEnv reads the GRPC_TLS_* env surface into TLSClientOptions:
// GRPC_TLS_CA, GRPC_TLS_CLIENT_CERT, GRPC_TLS_CLIENT_KEY, GRPC_TLS_SERVER_NAME.
func TLSFromEnv(getenv func(string) string) TLSClientOptions {
	return TLSClientOptions{
		CAFile:     getenv("GRPC_TLS_CA"),
		CertFile:   getenv("GRPC_TLS_CLIENT_CERT"),
		KeyFile:    getenv("GRPC_TLS_CLIENT_KEY"),
		ServerName: getenv("GRPC_TLS_SERVER_NAME"),
	}
}

// InsecureFromEnv reports the GRPC_INSECURE escape hatch: plaintext dials
// only when explicitly set to 1/true/yes. Never the default.
func InsecureFromEnv(getenv func(string) string) bool {
	switch getenv("GRPC_INSECURE") {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
		return true
	default:
		return false
	}
}

// loadCertPool reads the CA bundle (or falls back to the system pool).
func loadCertPool(caFile string) (*x509.CertPool, error) {
	if caFile == "" {
		return x509.NewCertPool(), nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("%w: read CA %q: %v", ErrTLSConfig, caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%w: no certificates in %q", ErrTLSConfig, caFile)
	}
	return pool, nil
}

// ClientCredentials resolves the transport credentials posture for dials:
//   - insecure escape hatch set -> plaintext (explicit local-dev opt-in),
//   - otherwise TLS verifying the server against the CA bundle (system
//     pool when GRPC_TLS_CA is unset), with optional client cert (mTLS).
//
// The returned error is a boot-time failure: callers must exit non-zero
// rather than fall back to plaintext.
func ClientCredentials(insecure bool, opts TLSClientOptions) (*tls.Config, error) {
	if insecure {
		return &tls.Config{}, nil
	}
	pool, err := loadCertPool(opts.CAFile)
	if err != nil {
		return nil, err
	}
	cfg := tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}
	if opts.ServerName != "" {
		cfg.ServerName = opts.ServerName
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%w: load client cert: %v", ErrTLSConfig, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return &cfg, nil
}

// ServerCredentials resolves the listener-side posture: TLS when both
// GRPC_TLS_CERT and GRPC_TLS_KEY are configured, plaintext otherwise (dev
// default; compose and the k8s manifests choose the posture).
func ServerCredentials(certFile, keyFile string) (*tls.Config, bool, error) {
	if certFile == "" && keyFile == "" {
		return nil, false, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, false, fmt.Errorf("%w: GRPC_TLS_CERT and GRPC_TLS_KEY must be set together", ErrTLSConfig)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, false, fmt.Errorf("%w: load server cert: %v", ErrTLSConfig, err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}, true, nil
}

// DialFromEnv is the env-driven dial helper: it resolves the GRPC_TLS_* /
// GRPC_INSECURE posture (os.Getenv) and returns a connected client with the
// right transport credentials. A TLS configuration error is returned, never
// silently downgraded to plaintext. Extra opts (interceptors) are
// caller-owned.
func DialFromEnv(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	insecureDial := InsecureFromEnv(os.Getenv)
	var creds credentials.TransportCredentials
	if insecureDial {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // explicit GRPC_INSECURE=1 escape hatch
	} else {
		cfg, err := ClientCredentials(false, TLSFromEnv(os.Getenv))
		if err != nil {
			return nil, err
		}
		creds = credentials.NewTLS(cfg)
	}
	opts = append(opts, grpc.WithTransportCredentials(creds))
	return Dial(addr, opts...)
}

// ServerOptionFromEnv resolves the GRPC_TLS_CERT/GRPC_TLS_KEY listener
// posture into a grpc.ServerOption. The bool reports whether TLS is on; a
// misconfiguration (half-set, unloadable) is an error, not a fallback.
func ServerOptionFromEnv() (grpc.ServerOption, bool, error) {
	cfg, ok, err := ServerCredentials(os.Getenv("GRPC_TLS_CERT"), os.Getenv("GRPC_TLS_KEY"))
	if err != nil || !ok {
		return nil, false, err
	}
	return grpc.Creds(credentials.NewTLS(cfg)), true, nil
}
