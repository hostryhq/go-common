package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) Lookup {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestLoaderCollectsAllErrorsWithoutEchoingValues(t *testing.T) {
	l := New(env(map[string]string{"PORT": "abc-secret", "TIMEOUT": "10x", "DEBUG": "maybe"}))
	l.Int("PORT", 8080)
	l.Duration("TIMEOUT", time.Second)
	l.Bool("DEBUG", false)
	l.Required("DB_URL")
	err := l.Err()
	if err == nil {
		t.Fatal("expected errors")
	}
	msg := err.Error()
	for _, k := range []string{"PORT", "TIMEOUT", "DEBUG", "DB_URL"} {
		if !strings.Contains(msg, k) {
			t.Errorf("missing %s in %q", k, msg)
		}
	}
	if strings.Contains(msg, "abc-secret") {
		t.Errorf("value echoed: %q", msg)
	}
}

func TestLoaderDefaultsAndParsing(t *testing.T) {
	l := New(env(map[string]string{"N": "7", "D": "1m", "L": "a, b,,c"}))
	if l.Int("N", 1) != 7 || l.Int("M", 3) != 3 || l.Duration("D", 0) != time.Minute || len(l.List("L")) != 3 {
		t.Fatal("unexpected values")
	}
	if l.Err() != nil {
		t.Fatal(l.Err())
	}
}
