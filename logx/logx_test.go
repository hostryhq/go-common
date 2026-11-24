package logx

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/tenancy"
)

func TestRedactsSensitiveKeysButNotLookalikes(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo, "svc")
	l.Info("x", "password", "p", "Authorization", "Bearer abc", "customer_phone", "999",
		"pin", "1234", "shipping_method", "express", "company_id", "c1",
		slog.Group("g", slog.String("api_token", "t")))
	out := buf.String()
	for _, leak := range []string{`"p"`, "Bearer", "999", "1234", `"t"`} {
		if strings.Contains(out, leak) {
			t.Errorf("leaked %s in %s", leak, out)
		}
	}
	for _, keep := range []string{"express", "c1"} {
		if !strings.Contains(out, keep) {
			t.Errorf("over-redacted %s in %s", keep, out)
		}
	}
}

func TestAttachesRequestAndTenantIDs(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, slog.LevelInfo, "svc")
	tid := ids.New()
	ctx := WithRequestID(tenancy.With(context.Background(), tenancy.Context{TenantID: tid, ActorID: "m1"}), "req-1")
	l.InfoContext(ctx, "hello")
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if m["request_id"] != "req-1" || m["tenant_id"] != tid.String() || m["actor_id"] != "m1" || m["service"] != "svc" {
		t.Fatalf("log = %v", m)
	}
}
