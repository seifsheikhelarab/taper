package idemprune

import (
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}.withDefaults()
	if cfg.Interval != time.Hour {
		t.Fatalf("interval = %v, want 1h", cfg.Interval)
	}
	if cfg.Retention != 7*24*time.Hour {
		t.Fatalf("retention = %v, want 7d", cfg.Retention)
	}
	if cfg.BatchSize != 1000 {
		t.Fatalf("batch = %d, want 1000", cfg.BatchSize)
	}
	custom := Config{Interval: -1, Retention: -2, BatchSize: -3}.withDefaults()
	if custom.Interval != time.Hour || custom.Retention != 7*24*time.Hour || custom.BatchSize != 1000 {
		t.Fatalf("negative values must fall back to defaults: %+v", custom)
	}
}
