package config

import (
	"errors"
	"testing"
	"time"
)

func TestEnvRequired(t *testing.T) {
	t.Setenv("CFG_REQ", "value")
	v, err := EnvRequired("CFG_REQ")
	if err != nil || v != "value" {
		t.Fatalf("got %q, %v", v, err)
	}
	if _, err := EnvRequired("CFG_MISSING"); err == nil {
		t.Fatal("missing key should error")
	}
}

func TestEnvFloatFailFast(t *testing.T) {
	cases := []struct {
		value string
		want  float64
		noerr bool
	}{
		{"10", 10, true},
		{"0.5", 0.5, true},
		{"", 0, false},    // unset -> error
		{"abc", 0, false}, // unparseable -> error (was silent default pre-T5)
		{"-1", 0, false},  // non-positive -> error
		{"0", 0, false},   // non-positive -> error
	}
	for _, tc := range cases {
		t.Run("value="+tc.value, func(t *testing.T) {
			if tc.value == "" {
				t.Setenv("CFG_F", "")
			} else {
				t.Setenv("CFG_F", tc.value)
			}
			got, err := EnvFloat("CFG_F")
			if tc.noerr {
				if err != nil || got != tc.want {
					t.Fatalf("got %v, %v; want %v, nil", got, err, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("value %q should error", tc.value)
			}
		})
	}
}

func TestEnvIntFailFast(t *testing.T) {
	t.Setenv("CFG_I", "42")
	if v, err := EnvPositiveInt("CFG_I"); err != nil || v != 42 {
		t.Fatalf("got %v, %v", v, err)
	}
	t.Setenv("CFG_I", "3.5")
	if _, err := EnvPositiveInt("CFG_I"); err == nil {
		t.Fatal("float string should error")
	}
	t.Setenv("CFG_I", "")
	if _, err := EnvPositiveInt("CFG_I"); err == nil {
		t.Fatal("unset should error")
	}
}

func TestEnvDurationFailFast(t *testing.T) {
	t.Setenv("CFG_D", "30s")
	if d, err := EnvDurationRequired("CFG_D"); err != nil || d != 30*time.Second {
		t.Fatalf("got %v, %v", d, err)
	}
	t.Setenv("CFG_D", "later")
	if _, err := EnvDurationRequired("CFG_D"); err == nil {
		t.Fatal("unparseable should error")
	}
	t.Setenv("CFG_D", "0s")
	if _, err := EnvDurationRequired("CFG_D"); err == nil {
		t.Fatal("non-positive should error")
	}
}

func TestCollect(t *testing.T) {
	if err := Collect(nil, nil); err != nil {
		t.Fatalf("no problems: %v", err)
	}
	err := Collect(errors.New("a missing"), errors.New("b invalid"), nil)
	if err == nil {
		t.Fatal("problems should error")
	}
	var ve *ValidateError
	if !errors.As(err, &ve) || len(ve.Problems) != 2 {
		t.Fatalf("want ValidateError with 2 problems, got %v", err)
	}
}
