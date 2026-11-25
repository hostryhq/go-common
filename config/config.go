// Package config loads typed configuration from environment variables only
// (12-factor). Errors are collected so startup reports every problem at once
// and then fails fast. Secrets are never defaulted or echoed in errors.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Lookup matches os.LookupEnv.
type Lookup func(key string) (string, bool)

// Loader reads variables and accumulates errors.
type Loader struct {
	lookup Lookup
	errs   []error
}

// New returns a Loader over lookup (pass os.LookupEnv).
func New(lookup Lookup) *Loader { return &Loader{lookup: lookup} }

// Err returns all accumulated problems joined, or nil. Call once after
// loading and exit on non-nil.
func (l *Loader) Err() error { return errors.Join(l.errs...) }

func (l *Loader) get(key string) (string, bool) {
	v, ok := l.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

func (l *Loader) fail(key, msg string) {
	l.errs = append(l.errs, fmt.Errorf("config %s: %s", key, msg))
}

// String returns the variable or def when unset.
func (l *Loader) String(key, def string) string {
	if v, ok := l.get(key); ok {
		return v
	}
	return def
}

// Required returns the variable, recording an error when unset. Use for
// anything without a safe default, including all secrets.
func (l *Loader) Required(key string) string {
	v, ok := l.get(key)
	if !ok {
		l.fail(key, "required but not set")
	}
	return v
}

// Int parses an integer.
func (l *Loader) Int(key string, def int) int {
	v, ok := l.get(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(key, "not an integer")
		return def
	}
	return n
}

// Bool parses a boolean.
func (l *Loader) Bool(key string, def bool) bool {
	v, ok := l.get(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(key, "not a boolean")
		return def
	}
	return b
}

// Duration parses a Go duration such as "30s".
func (l *Loader) Duration(key string, def time.Duration) time.Duration {
	v, ok := l.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(key, "not a duration (e.g. 30s)")
		return def
	}
	return d
}

// List parses a comma-separated list, dropping empty items.
func (l *Loader) List(key string) []string {
	v, ok := l.get(key)
	if !ok {
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
