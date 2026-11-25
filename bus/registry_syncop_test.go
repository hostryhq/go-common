package bus

import (
	"os"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

func TestRegistryAcceptsOnlySyncOpAsTwoSegmentType(t *testing.T) {
	op := &fstest.MapFile{Data: []byte("$id: urn:hostry:events:sync.op:v1\nx-publisher: sync\nx-subscribers: [billing, orders]\n" +
		"x-subscribers-by-namespace:\n  bill: [billing, orders]\n  kot: [orders]\n")}
	r, err := LoadRegistry(fstest.MapFS{"sync/op.v1.yaml": op,
		"sync/op.flagged.v1.yaml": schema("sync.op.flagged:v1", "sync", "[orders]", "")})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range r.Consumptions() {
		if c.Consumer == "orders" && !slices.Equal(c.Subjects, []string{"evt.sync.op.bill.v1", "evt.sync.op.kot.v1", "evt.sync.op.flagged.v1"}) {
			t.Errorf("orders subjects = %v", c.Subjects)
		}
		if c.Consumer == "billing" && !slices.Equal(c.Subjects, []string{"evt.sync.op.bill.v1"}) {
			t.Errorf("billing subjects = %v", c.Subjects)
		}
	}
	if !slices.Contains(r.PermissionsFor("sync").Publish, "evt.sync.>") {
		t.Error("sync must publish evt.sync.>")
	}
	for name, id := range map[string]struct{ id, pub string }{
		"other two-segment": {"identity.op:v1", "identity"},
		"sync.op by other":  {"sync.op:v1", "orders"},
		"four segments":     {"sync.op.a.b:v1", "sync"},
	} {
		if _, err := LoadRegistry(fstest.MapFS{"x/a.v1.yaml": schema(id.id, id.pub, "[]", "")}); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
}

// HOSTRY_EVENTS_DIR overrides the default sibling api-specs-p2 checkout (b153c62).
func TestRegistryLoadsFullPhase2EventsDir(t *testing.T) {
	dir := os.Getenv("HOSTRY_EVENTS_DIR")
	if dir == "" {
		dir = "../../api-specs-p2/events"
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("events dir %s not present", dir)
	}
	r, err := LoadRegistry(os.DirFS(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(r.Publishers(), "sync") {
		t.Errorf("publishers %v lack sync", r.Publishers())
	}
	p := r.PermissionsFor("orders")
	if !slices.ContainsFunc(p.Publish, func(s string) bool { return strings.Contains(s, "EVT_SYNC.orders") }) {
		t.Errorf("orders has no consumer on EVT_SYNC: %v", p.Publish)
	}
	k, err := GenerateKeys(r.Services())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WriteDevFiles(t.TempDir(), k); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRejectsGatewayEventNameAsNamespace(t *testing.T) {
	op := &fstest.MapFile{Data: []byte("$id: urn:hostry:events:sync.op:v1\nx-publisher: sync\nx-subscribers: [orders]\n" +
		"x-subscribers-by-namespace:\n  flagged: [orders]\n")}
	if _, err := LoadRegistry(fstest.MapFS{"sync/op.v1.yaml": op}); err == nil {
		t.Fatal("flagged is the gateway event sync.op.flagged and must not be an op namespace")
	}
}
