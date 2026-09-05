package outboxprune

import (
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}.withDefaults()
	if cfg.Interval != time.Hour || cfg.Retention != 72*time.Hour || cfg.BatchSize != 1000 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}
