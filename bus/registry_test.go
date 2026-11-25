package bus

import (
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

func schema(id, pub, subs, signal string) *fstest.MapFile {
	s := "$id: urn:hostry:events:" + id + "\nx-publisher: " + pub + "\nx-subscribers: " + subs + "\n"
	if signal != "" {
		s += "x-signal-listeners: " + signal + "\n"
	}
	return &fstest.MapFile{Data: []byte(s)}
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	r, err := LoadRegistry(fstest.MapFS{
		"_envelope.v1.yaml":               {Data: []byte("$id: urn:hostry:events:envelope:v1\n")},
		"identity/device.revoked.v1.yaml": schema("identity.device.revoked:v1", "identity", "[sync]", "[sync]"),
		"identity/member.changed.v1.yaml": schema("identity.member.changed:v1", "identity", "[sync, tenancy]", ""),
		"tenancy/link.revoked.v1.yaml":    schema("tenancy.link.revoked:v1", "tenancy", "[identity]", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRegistryDerivesConsumersAndStreams(t *testing.T) {
	r := testRegistry(t)
	if got, want := r.Publishers(), []string{"identity", "tenancy"}; !slices.Equal(got, want) {
		t.Fatalf("publishers %v want %v", got, want)
	}
	var sync *Consumption
	for _, c := range r.Consumptions() {
		if c.Consumer == "sync" && c.Stream == "EVT_IDENTITY" {
			sync = &c
		}
	}
	if sync == nil || !slices.Equal(sync.Subjects, []string{"evt.identity.device.revoked.v1", "evt.identity.member.changed.v1"}) {
		t.Fatalf("sync consumer on EVT_IDENTITY = %+v", sync)
	}
}

func TestRegistryRejectsTypeNotPrefixedByPublisher(t *testing.T) {
	_, err := LoadRegistry(fstest.MapFS{"x/a.v1.yaml": schema("identity.device.revoked:v1", "tenancy", "[]", "")})
	if err == nil {
		t.Fatal("expected error: publisher does not own the type")
	}
}

func TestPermissionsAreMinimalAndDefaultDeny(t *testing.T) {
	r := testRegistry(t)
	sync := r.PermissionsFor("sync")
	if slices.ContainsFunc(sync.Publish, func(s string) bool { return strings.HasPrefix(s, "evt.") }) {
		t.Errorf("sync publishes nothing, got %v", sync.Publish)
	}
	for _, p := range sync.Publish {
		if strings.Contains(p, "EVT_TENANCY") || strings.Contains(p, "STREAM") || strings.Contains(p, "CREATE") {
			t.Errorf("sync must not touch other streams or create anything: %s", p)
		}
	}
	if !slices.Contains(sync.Publish, "$JS.API.CONSUMER.MSG.NEXT.EVT_IDENTITY.sync") {
		t.Errorf("sync must pull its own consumer: %v", sync.Publish)
	}
	if !slices.Equal(sync.Subscribe, []string{"_INBOX.sync.>", "evt.identity.device.revoked.v1"}) {
		t.Errorf("sync subscribe = %v", sync.Subscribe)
	}
	id := r.PermissionsFor("identity")
	if id.Publish[0] != "evt.identity.>" {
		t.Errorf("identity publish = %v", id.Publish)
	}
}

func TestRenderedConfigHoldsOnlyPublicKeys(t *testing.T) {
	r := testRegistry(t)
	k, err := GenerateKeys(r.Services())
	if err != nil {
		t.Fatal(err)
	}
	cfg := r.RenderConfig(k.Pubs)
	for name, seed := range k.Seeds {
		if strings.Contains(cfg, string(seed)) {
			t.Fatalf("config leaks the seed of %s", name)
		}
		if !strings.Contains(cfg, k.Pubs[name]) {
			t.Fatalf("config lacks public key of %s", name)
		}
	}
}
