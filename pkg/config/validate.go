package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// ValidateError reports every env problem found at boot in one message
// (spec #52, B2: fail loud, never silently fall back to a default).
type ValidateError struct {
	Problems []string
}

func (e *ValidateError) Error() string {
	msg := "invalid configuration:"
	for _, p := range e.Problems {
		msg += "\n  - " + p
	}
	return msg
}

// EnvRequired returns key's value or an error naming the missing variable.
func EnvRequired(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("%s is required but not set", key)
	}
	return v, nil
}

// EnvFloat returns key parsed as a float. Unset is an error; unparseable or
// non-positive values are errors too — no silent default.
func EnvFloat(key string) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, fmt.Errorf("%s is required but not set", key)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a number", key, v)
	}
	if f <= 0 {
		return 0, fmt.Errorf("%s=%q must be > 0", key, v)
	}
	return f, nil
}

// EnvPositiveInt returns key parsed as a positive int with the same
// fail-fast rules as EnvFloat (the legacy EnvInt keeps its fallback
// behavior for non-critical values).
func EnvPositiveInt(key string) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, fmt.Errorf("%s is required but not set", key)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not an integer", key, v)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s=%q must be > 0", key, v)
	}
	return n, nil
}

// EnvDurationRequired returns key parsed as a duration with the same
// fail-fast rules as EnvFloat (the legacy EnvDuration keeps its fallback
// behavior for non-critical values).
func EnvDurationRequired(key string) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return 0, fmt.Errorf("%s is required but not set", key)
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not a duration (use e.g. 30s, 5m)", key, v)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s=%q must be > 0", key, v)
	}
	return d, nil
}

// Collect joins per-variable errors into one ValidateError (nil when all
// accessors succeeded), so a boot reports everything wrong at once.
func Collect(errs ...error) error {
	var problems []string
	for _, err := range errs {
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return &ValidateError{Problems: problems}
}
