// Package bustest starts the infra/dev compose stack (NATS JetStream + Postgres) for the
// bus, outbox and inbox integration tests. Dev credentials are generated per run; nothing
// here is a real secret. Tests skip when Docker or the infra and api-specs clones are missing.
package bustest

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/hostryhq/go-common/bus"
	"github.com/hostryhq/go-common/inbox"
	"github.com/hostryhq/go-common/outbox"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ProbeNoInfo is a test-only user: the sync permissions without consumer-info.
const ProbeNoInfo = "probe-noinfo"

// Env is a running dev stack.
type Env struct {
	Registry  *bus.Registry
	NATSURL   string
	PGAdmin   *sql.DB
	pgDSNBase string
	confDir   string
	project   string
	compose   string
	dbSeq     int
	mu        sync.Mutex
}

var (
	once    sync.Once
	shared  *Env
	skipMsg string
)

// Shared returns the stack for the test binary, starting it on first use, or skips the test.
func Shared(t testing.TB) *Env {
	t.Helper()
	once.Do(func() {
		e, msg := start()
		shared, skipMsg = e, msg
	})
	if shared == nil {
		t.Skip(skipMsg)
	}
	return shared
}

// Teardown stops the stack; call from TestMain after m.Run.
func Teardown() {
	if shared != nil {
		shared.stop()
	}
}

func repoRoot() string {
	wd, _ := os.Getwd()
	for d := wd; d != "/"; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
	}
	return wd
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func start() (*Env, string) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, "docker not available"
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		return nil, "docker daemon not reachable"
	}
	root := repoRoot()
	infra := getenv("HOSTRY_INFRA_DIR", filepath.Join(root, "..", "infra"))
	events := getenv("HOSTRY_EVENTS_DIR", filepath.Join(root, "..", "api-specs", "events"))
	compose := filepath.Join(infra, "dev", "compose.yaml")
	if _, err := os.Stat(compose); err != nil {
		return nil, "infra/dev/compose.yaml not found (set HOSTRY_INFRA_DIR)"
	}
	reg, err := bus.LoadRegistry(os.DirFS(events))
	if err != nil {
		return nil, "api-specs events not loadable: " + err.Error()
	}
	conf, err := os.MkdirTemp("", "hostry-bustest-*")
	if err != nil {
		return nil, err.Error()
	}
	// probeNoInfo is the sync service minus CONSUMER.INFO, to find out whether the client needs it (r15 #31).
	probe := reg.PermissionsFor("sync")
	var kept []string
	for _, s := range probe.Publish {
		if !strings.HasPrefix(s, "$JS.API.CONSUMER.INFO.") {
			kept = append(kept, s)
		}
	}
	probe.Publish = kept
	extra := []bus.ExtraUser{{Name: ProbeNoInfo, Perms: probe}}
	keys, err := bus.GenerateKeys(append(reg.Services(), ProbeNoInfo))
	if err != nil {
		return nil, err.Error()
	}
	if err := reg.WriteDevFiles(conf, keys, extra...); err != nil {
		return nil, err.Error()
	}
	np, _ := freePort()
	pp, _ := freePort()
	e := &Env{Registry: reg, confDir: conf, compose: compose, project: fmt.Sprintf("hostrytest%d", os.Getpid())}
	cmd := exec.Command("docker", "compose", "-f", compose, "-p", e.project, "up", "-d", "--wait")
	cmd.Env = append(os.Environ(), "HOSTRY_NATS_CONF_DIR="+conf, fmt.Sprintf("HOSTRY_NATS_PORT=%d", np), fmt.Sprintf("HOSTRY_PG_PORT=%d", pp))
	if out, err := cmd.CombinedOutput(); err != nil {
		e.stop()
		return nil, fmt.Sprintf("compose up failed: %v: %s", err, out)
	}
	e.NATSURL = fmt.Sprintf("nats://127.0.0.1:%d", np)
	e.pgDSNBase = fmt.Sprintf("postgres://hostry:hostry-dev-only@127.0.0.1:%d/", pp)
	e.PGAdmin, err = sql.Open("pgx", e.pgDSNBase+"hostry")
	if err != nil {
		e.stop()
		return nil, err.Error()
	}
	for i := 0; i < 50; i++ {
		if err = e.PGAdmin.Ping(); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		e.stop()
		return nil, "postgres not ready: " + err.Error()
	}
	return e, ""
}

func (e *Env) stop() {
	if e.PGAdmin != nil {
		e.PGAdmin.Close()
	}
	cmd := exec.Command("docker", "compose", "-f", e.compose, "-p", e.project, "down", "-v", "--timeout", "2")
	cmd.Env = append(os.Environ(), "HOSTRY_NATS_CONF_DIR="+e.confDir)
	_ = cmd.Run()
	_ = os.RemoveAll(e.confDir)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// SeedFile is the dev seed file of a service ("admin" for the provisioning user).
func (e *Env) SeedFile(service string) string {
	return filepath.Join(e.confDir, "seeds", service+".nk")
}

// Connect opens a connection as service.
func (e *Env) Connect(t testing.TB, service string, opts ...nats.Option) *nats.Conn {
	t.Helper()
	var nc *nats.Conn
	var err error
	switch service {
	case bus.AdminUser:
		nc, err = bus.ConnectAdmin(e.NATSURL, e.SeedFile(service), opts...)
	case ProbeNoInfo: // same reply prefix as sync
		nc, err = bus.Connect(e.NATSURL, "sync", e.SeedFile(service), opts...)
	default:
		nc, err = bus.Connect(e.NATSURL, service, e.SeedFile(service), opts...)
	}
	if err != nil {
		t.Fatalf("connect %s: %v", service, err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// ResetBus deletes every stream and re-provisions from the registry.
func (e *Env) ResetBus(t testing.TB, o bus.ProvisionOptions) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	nc := e.Connect(t, bus.AdminUser)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	lister := js.StreamNames(ctx)
	for name := range lister.Name() {
		if err := js.DeleteStream(ctx, name); err != nil {
			t.Fatalf("delete stream %s: %v", name, err)
		}
	}
	if err := bus.Provision(ctx, nc, e.Registry, o); err != nil {
		t.Fatalf("provision: %v", err)
	}
}

// NewDB creates a fresh database with the outbox and inbox schemas.
func (e *Env) NewDB(t testing.TB) *sql.DB {
	t.Helper()
	e.mu.Lock()
	e.dbSeq++
	name := fmt.Sprintf("t%d_%d", os.Getpid(), e.dbSeq)
	e.mu.Unlock()
	if _, err := e.PGAdmin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", e.pgDSNBase+name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, s := range []string{outbox.Schema, inbox.Schema} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
