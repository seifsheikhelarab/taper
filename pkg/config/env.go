package config

import (
	"os"
	"strconv"
	"time"
)

// EnvOr returns the value of the environment variable named by key,
// or def if the variable is empty or not set.
func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvBool reports whether key is set to a truthy value ("1", "true", "yes",
// case-insensitive).
func EnvBool(key string) bool {
	switch os.Getenv(key) {
	case "1", "true", "TRUE", "True", "yes", "YES", "Yes":
		return true
	default:
		return false
	}
}

// EnvDuration returns key parsed as a duration, or def when unset, invalid,
// or non-positive.
func EnvDuration(key string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(os.Getenv(key))
	if err != nil || d <= 0 {
		return def
	}
	return d
}

// EnvInt returns key parsed as an int, or def when unset, invalid, or
// non-positive.
func EnvInt(key string, def int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n <= 0 {
		return def
	}
	return n
}
