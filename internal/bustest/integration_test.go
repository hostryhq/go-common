//go:build integration

package bustest_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hostryhq/go-common/bus"
	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/inbox"
	"github.com/hostryhq/go-common/internal/bustest"
	"github.com/hostryhq/go-common/outbox"
	"github.com/hostryhq/go-common/tenancy"
)

func TestMain(m *testing.M) {
	code := m.Run()
	bustest.Teardown()
	os.Exit(code)
}

var tenantA, tenantB = ids.New(), ids.New()

func deviceRevoked(tenant ids.ID, key string, keySeq int64) events.Envelope {
	return events.Envelope{
		ID: ids.New(), Type: "identity.device.revoked", V: 1, Source: "identity",
		TenantID: tenant, Key: key, KeySeq: keySeq, OccurredAt: time.Now(),
		Data: json.RawMessage(`{"device_id":"` + key + `","reason":"lost"}`),
	}
}

func commit(t *testing.T, db *sql.DB, evs ...events.Envelope) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		if err := outbox.Publish(context.Background(), tx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, db *sql.DB, q string, args ...any) (n int) {
	t.Helper()
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func streamMsgs(t *testing.T, env *bustest.Env, stream string) uint64 {
	t.Helper()
	js, _ := jetstream.New(env.Connect(t, bus.AdminUser))
	s, err := js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info.State.Msgs
}

// startRelay runs a relay for the identity service database against the real bus.
func startRelay(t *testing.T, env *bustest.Env, db *sql.DB, sink outbox.Sink, lock int64) *outbox.Relay {
	t.Helper()
	if sink == nil {
		pub, err := bus.NewPublisher(env.Connect(t, "identity"))
		if err != nil {
			t.Fatal(err)
		}
		sink = pub
	}
	r := &outbox.Relay{DB: db, Sink: sink, LockKey: lock, Poll: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return r
}

// subscriber builds an inbox subscriber for sync reading identity events, with a handler that
// records applied events per key (highest key_seq wins, as a state projection would).
type projection struct {
	mu      sync.Mutex
	applied map[string]int64
	tenants []ids.ID
	calls   atomic.Int64
}

func (p *projection) handle(ctx context.Context, tx *sql.Tx, e events.Envelope) error {
	p.calls.Add(1)
	tc, ok := tenancy.From(ctx)
	if !ok || tc.TenantID != e.TenantID {
		return errors.New("tenant context not set from the envelope")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenants = append(p.tenants, tc.TenantID)
	if inbox.Newer(e.KeySeq, p.applied[e.Key]) {
		p.applied[e.Key] = e.KeySeq
	}
	return nil
}

func (p *projection) get(key string) int64 { p.mu.Lock(); defer p.mu.Unlock(); return p.applied[key] }

func newProjection() *projection { return &projection{applied: map[string]int64{}} }

func runSubscriber(t *testing.T, env *bustest.Env, db *sql.DB, h inbox.Handler, tweak func(*inbox.Subscriber)) *inbox.Subscriber {
	t.Helper()
	cons, err := bus.BindConsumer(context.Background(), env.Connect(t, "sync"), "identity", "sync")
	if err != nil {
		t.Fatal(err)
	}
	s := &inbox.Subscriber{DB: db, Consumer: "sync", Handle: h}
	if tweak != nil {
		tweak(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx, cons.Source(200*time.Millisecond)); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return s
}

// --- unverified NATS facts (r15 #30, #31) -----------------------------------------------

func TestNATSVersionAndNkeyStaticConfig(t *testing.T) {
	env := bustest.Shared(t)
	nc := env.Connect(t, "identity") // nkey user from static config
	t.Logf("server version %s", nc.ConnectedServerVersion())
	if v := nc.ConnectedServerVersion(); v != "2.15.3" {
		t.Errorf("expected pinned 2.15.3, got %s", v)
	}
	// A seed that is not in the static config must be refused.
	other, _ := bus.GenerateKeys([]string{"x"})
	dir := t.TempDir()
	if err := env.Registry.WriteDevFiles(dir, other); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Connect(env.NATSURL, "x", dir+"/seeds/x.nk"); err == nil {
		t.Fatal("unknown nkey must not authenticate")
	}
}

func TestConsumerInfoSubjectNeededForBinding(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{})
	_, errWithout := bus.BindConsumer(context.Background(), env.Connect(t, bustest.ProbeNoInfo), "identity", "sync")
	_, errWith := bus.BindConsumer(context.Background(), env.Connect(t, "sync"), "identity", "sync")
	t.Logf("bind without $JS.API.CONSUMER.INFO: %v", errWithout)
	t.Logf("bind with    $JS.API.CONSUMER.INFO: %v", errWith)
	if errWith != nil {
		t.Fatalf("the generated permissions must allow binding: %v", errWith)
	}
	if errWithout == nil {
		t.Log("FINDING: nats.go jetstream.Consumer() binding does NOT need CONSUMER.INFO")
	} else {
		t.Log("FINDING: nats.go jetstream.Consumer() binding NEEDS $JS.API.CONSUMER.INFO.<stream>.<consumer>")
	}
}

// --- E6 access control --------------------------------------------------------------------

func TestE6AccessControl(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Denied operations get no reply from the server, so they are bounded by a short deadline.
	// Each denied call gets its own deadline so one expiry cannot mask the next check.
	denied := func() context.Context {
		c, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		t.Cleanup(cancel)
		return c
	}

	violations := make(chan string, 16)
	onErr := nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err.Error() })

	// identity publishes on its own subjects.
	idPub, err := bus.NewPublisher(env.Connect(t, "identity"))
	if err != nil {
		t.Fatal(err)
	}
	if err := idPub.Publish(ctx, "evt.identity.device.revoked.v1", "own-1", mustMarshal(t, deviceRevoked(tenantA, "d", 1))); err != nil {
		t.Fatalf("identity must publish on its own subject: %v", err)
	}

	// identity cannot publish on tenancy's subjects: no stream ack, permissions violation, stream unchanged.
	before := streamMsgs(t, env, "EVT_TENANCY")
	idForeign, _ := bus.NewPublisher(env.Connect(t, "identity", onErr))
	err = idForeign.Publish(denied(), "evt.tenancy.link.revoked.v1", "foreign-1", []byte(`{}`))
	if err == nil {
		t.Fatal("identity must not publish on evt.tenancy.>")
	}
	t.Logf("publish on another service's subject: %v", err)
	if streamMsgs(t, env, "EVT_TENANCY") != before {
		t.Fatal("message reached another service's stream")
	}
	select {
	case v := <-violations:
		t.Logf("server error: %s", v)
	case <-time.After(2 * time.Second):
		t.Error("expected an async permissions violation")
	}

	// sync cannot publish on identity's subjects either.
	syncPub, _ := bus.NewPublisher(env.Connect(t, "sync"))
	if err := syncPub.Publish(denied(), "evt.identity.device.revoked.v1", "sync-1", []byte(`{}`)); err == nil {
		t.Fatal("sync must not publish on evt.identity.>")
	}

	// sync cannot read another service's consumer (identity's consumer on EVT_TENANCY).
	if _, err := bus.BindConsumer(denied(), env.Connect(t, "sync"), "tenancy", "identity"); err == nil {
		t.Fatal("sync must not bind identity's consumer")
	}
	// identity cannot read sync's consumer on its own stream.
	if _, err := bus.BindConsumer(denied(), env.Connect(t, "identity"), "identity", "sync"); err == nil {
		t.Fatal("identity must not bind sync's consumer")
	}

	// no service can create a stream or a consumer.
	for _, svc := range []string{"identity", "sync", "tenancy"} {
		js, _ := jetstream.New(env.Connect(t, svc))
		if _, err := js.CreateStream(denied(), jetstream.StreamConfig{Name: "EVIL", Subjects: []string{"evil.>"}}); err == nil {
			t.Fatalf("%s created a stream", svc)
		}
		if _, err := js.CreateOrUpdateConsumer(denied(), "EVT_IDENTITY", jetstream.ConsumerConfig{Durable: "evil"}); err == nil {
			t.Fatalf("%s created a consumer", svc)
		}
		if err := js.DeleteStream(denied(), "EVT_IDENTITY"); err == nil {
			t.Fatalf("%s deleted a stream", svc)
		}
	}

	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	// the owner of a consumer can pull and acknowledge: after ack, nothing is redelivered.
	cons, err := bus.BindConsumer(ctx, env.Connect(t, "sync"), "identity", "sync")
	if err != nil {
		t.Fatal(err)
	}
	d, err := cons.Next(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("sync must pull its own consumer: %v", err)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack with generated $JS.ACK permission failed: %v", err)
	}
	if _, err := cons.Next(ctx, 500*time.Millisecond); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("acked message must not be redelivered, got %v", err)
	}
}

func mustMarshal(t *testing.T, e events.Envelope) []byte {
	t.Helper()
	b, err := e.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// --- E1 outbox atomicity -------------------------------------------------------------------

func TestE1OutboxAtomicity(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{})
	db := env.NewDB(t)
	startRelay(t, env, db, nil, 1)

	tx, _ := db.Begin()
	if err := outbox.Publish(context.Background(), tx, deviceRevoked(tenantA, "rolled-back", 1)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	commit(t, db, deviceRevoked(tenantA, "k1", 1), deviceRevoked(tenantA, "k2", 1))

	eventually(t, "two events published", func() bool { return streamMsgs(t, env, "EVT_IDENTITY") == 2 })
	eventually(t, "rows marked published", func() bool {
		return count(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 2
	})
	time.Sleep(300 * time.Millisecond)
	if n := streamMsgs(t, env, "EVT_IDENTITY"); n != 2 {
		t.Fatalf("exactly the committed events must exist, stream holds %d", n)
	}
	if n := count(t, db, `SELECT count(*) FROM outbox`); n != 2 {
		t.Fatalf("rolled-back event left a row (%d rows)", n)
	}
}

// --- E2 relay crash between publish and mark ----------------------------------------------

// crashySink publishes, then reports a failure once per event, as a crash before the row is marked.
type crashySink struct {
	inner outbox.Sink
	seen  sync.Map
}

func (c *crashySink) Publish(ctx context.Context, subject, id string, data []byte) error {
	if err := c.inner.Publish(ctx, subject, id, data); err != nil {
		return err
	}
	if _, again := c.seen.LoadOrStore(id, true); !again {
		return errors.New("simulated crash after publish, before mark")
	}
	return nil
}

func TestE2RelayCrashLosesNothingAndStreamDeduplicates(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{})
	db := env.NewDB(t)
	pub, _ := bus.NewPublisher(env.Connect(t, "identity"))
	startRelay(t, env, db, &crashySink{inner: pub}, 2)

	commit(t, db, deviceRevoked(tenantA, "a", 1), deviceRevoked(tenantA, "b", 1), deviceRevoked(tenantA, "c", 1))
	eventually(t, "all rows marked after retries", func() bool {
		return count(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 3
	})
	if n := streamMsgs(t, env, "EVT_IDENTITY"); n != 3 {
		t.Fatalf("each event was published twice but Nats-Msg-Id must de-duplicate: stream holds %d, want 3", n)
	}
}

func TestOnlyOneRelayIsActivePerDatabase(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{})
	db := env.NewDB(t)
	var calls atomic.Int64
	counting := sinkFunc(func(ctx context.Context, subject, id string, data []byte) error { calls.Add(1); return nil })
	startRelay(t, env, db, counting, 3)
	startRelay(t, env, db, counting, 3)
	startRelay(t, env, db, counting, 3)
	var evs []events.Envelope
	for i := 0; i < 20; i++ {
		evs = append(evs, deviceRevoked(tenantA, "k", int64(i+1)))
	}
	commit(t, db, evs...)
	eventually(t, "20 rows published", func() bool {
		return count(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 20
	})
	if calls.Load() != 20 {
		t.Fatalf("with advisory lock, each row is relayed once: %d calls", calls.Load())
	}
}

type sinkFunc func(ctx context.Context, subject, id string, data []byte) error

func (f sinkFunc) Publish(ctx context.Context, subject, id string, data []byte) error {
	return f(ctx, subject, id, data)
}

// --- E3 duplicate and reordered delivery, E8 tenant context --------------------------------

type fakeDelivery struct {
	data     []byte
	attempt  uint64
	acked    bool
	nakDelay time.Duration
	naked    bool
}

func (f *fakeDelivery) Subject() string           { return "evt.identity.device.revoked.v1" }
func (f *fakeDelivery) Data() []byte              { return f.data }
func (f *fakeDelivery) NumDelivered() uint64      { return f.attempt }
func (f *fakeDelivery) Ack() error                { f.acked = true; return nil }
func (f *fakeDelivery) Nak(d time.Duration) error { f.naked, f.nakDelay = true, d; return nil }

func TestE3DuplicateAndReorderedDeliveryConverge(t *testing.T) {
	env := bustest.Shared(t)
	db := env.NewDB(t)
	p := newProjection()
	s := &inbox.Subscriber{DB: db, Consumer: "sync", Handle: p.handle}
	e1, e2, e3 := deviceRevoked(tenantA, "dev", 1), deviceRevoked(tenantA, "dev", 2), deviceRevoked(tenantA, "dev", 3)
	for _, e := range []events.Envelope{e3, e1, e2, e2, e3, e1} { // reordered, with duplicates
		d := &fakeDelivery{data: mustMarshal(t, e), attempt: 1}
		if err := s.Process(context.Background(), d); err != nil || !d.acked {
			t.Fatalf("process: err=%v acked=%v", err, d.acked)
		}
	}
	if got := p.get("dev"); got != 3 {
		t.Fatalf("state must equal the in-order result: key_seq %d, want 3", got)
	}
	if n := p.calls.Load(); n != 3 {
		t.Fatalf("handler must run once per distinct event, ran %d", n)
	}
	if n := count(t, db, `SELECT count(*) FROM inbox`); n != 3 {
		t.Fatalf("inbox rows %d", n)
	}
}

func TestE8HandlerWritesUnderTheEnvelopeTenantOnly(t *testing.T) {
	env := bustest.Shared(t)
	db := env.NewDB(t)
	if _, err := db.Exec(`CREATE TABLE projection (tenant_id text, key text, PRIMARY KEY (tenant_id, key))`); err != nil {
		t.Fatal(err)
	}
	h := func(ctx context.Context, tx *sql.Tx, e events.Envelope) error {
		tc, err := tenancy.Require(ctx)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO projection VALUES ($1,$2)`, tc.TenantID.String(), e.Key)
		return err
	}
	s := &inbox.Subscriber{DB: db, Consumer: "sync", Handle: h}
	d := &fakeDelivery{data: mustMarshal(t, deviceRevoked(tenantA, "dev-a", 1)), attempt: 1}
	if err := s.Process(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT count(*) FROM projection WHERE tenant_id=$1`, tenantB.String()); n != 0 {
		t.Fatalf("event for tenant A wrote %d rows under tenant B", n)
	}
	if n := count(t, db, `SELECT count(*) FROM projection WHERE tenant_id=$1`, tenantA.String()); n != 1 {
		t.Fatalf("tenant A rows %d", n)
	}
}

// --- E4 consumer down then catches up -------------------------------------------------------

func TestE4ConsumerDownThenCatchesUpWithNoLoss(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{})
	db := env.NewDB(t)
	startRelay(t, env, db, nil, 4)
	var evs []events.Envelope
	for i := 0; i < 60; i++ {
		evs = append(evs, deviceRevoked(tenantA, "dev", int64(i+1)))
	}
	commit(t, db, evs...)
	eventually(t, "published while the consumer is down", func() bool { return streamMsgs(t, env, "EVT_IDENTITY") == 60 })

	consumerDB := env.NewDB(t)
	p := newProjection()
	runSubscriber(t, env, consumerDB, p.handle, nil)
	eventually(t, "consumer caught up", func() bool { return p.get("dev") == 60 })
	if n := count(t, consumerDB, `SELECT count(*) FROM inbox`); n != 60 {
		t.Fatalf("inbox rows %d, want 60", n)
	}
}

// --- redelivery, E9 poison event -------------------------------------------------------------

func TestRedeliveryAfterHandlerFailureThenSuccess(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{BackOff: []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}})
	db := env.NewDB(t)
	startRelay(t, env, db, nil, 5)
	commit(t, db, deviceRevoked(tenantA, "flaky", 1))

	var attempts atomic.Int64
	h := func(ctx context.Context, tx *sql.Tx, e events.Envelope) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	}
	consumerDB := env.NewDB(t)
	runSubscriber(t, env, consumerDB, h, func(s *inbox.Subscriber) {
		s.Backoff = func(uint64) time.Duration { return 50 * time.Millisecond }
	})
	eventually(t, "third attempt succeeds", func() bool {
		return count(t, consumerDB, `SELECT count(*) FROM inbox`) == 1
	})
	if n := count(t, consumerDB, `SELECT count(*) FROM event_parked`); n != 0 {
		t.Fatalf("a recovered event must not be parked (%d)", n)
	}
	// Failed attempts must not leave inbox rows behind (rollback): the row exists only after success.
	if attempts.Load() != 3 {
		t.Fatalf("attempts %d", attempts.Load())
	}
}

func TestE9PoisonEventParkedAndOtherKeysContinue(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{BackOff: []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond}, MaxDeliver: 3})
	db := env.NewDB(t)
	startRelay(t, env, db, nil, 6)
	poison := deviceRevoked(tenantA, "poison", 1)
	commit(t, db, poison, deviceRevoked(tenantA, "healthy", 1), deviceRevoked(tenantA, "healthy", 2))

	var alerts atomic.Int64
	p := newProjection()
	h := func(ctx context.Context, tx *sql.Tx, e events.Envelope) error {
		if e.Key == "poison" {
			return errors.New("cannot apply")
		}
		return p.handle(ctx, tx, e)
	}
	consumerDB := env.NewDB(t)
	runSubscriber(t, env, consumerDB, h, func(s *inbox.Subscriber) {
		s.MaxAttempts = 3
		s.Backoff = func(uint64) time.Duration { return 50 * time.Millisecond }
		s.OnPark = func(_, _, _ string) { alerts.Add(1) }
	})
	eventually(t, "healthy key processed", func() bool { return p.get("healthy") == 2 })
	eventually(t, "poison event parked", func() bool {
		return count(t, consumerDB, `SELECT count(*) FROM event_parked WHERE event_id=$1`, poison.ID.String()) == 1
	})
	if alerts.Load() != 1 {
		t.Fatalf("alert fired %d times", alerts.Load())
	}
	var attempts int
	_ = consumerDB.QueryRow(`SELECT attempts FROM event_parked`).Scan(&attempts)
	if attempts != 3 {
		t.Fatalf("parked after %d attempts, want 3", attempts)
	}
	if n := count(t, consumerDB, `SELECT count(*) FROM inbox WHERE event_id=$1`, poison.ID.String()); n != 0 {
		t.Fatal("poison event must not be recorded as processed")
	}
}

// --- r15 #33: discard new when full ----------------------------------------------------------

func TestStreamFullRefusesNewMessagesAndRelayKeepsRows(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{MaxBytes: 3000})
	pub, _ := bus.NewPublisher(env.Connect(t, "identity"))
	ctx := context.Background()

	var firstErr error
	published := 0
	for i := 0; i < 40 && firstErr == nil; i++ {
		e := deviceRevoked(tenantA, "k", int64(i+1))
		firstErr = pub.Publish(ctx, "evt.identity.device.revoked.v1", e.ID.String(), mustMarshal(t, e))
		if firstErr == nil {
			published++
		}
	}
	if firstErr == nil {
		t.Fatal("stream with MaxBytes and discard new must eventually refuse")
	}
	var api *jetstream.APIError
	code := -1
	if errors.As(firstErr, &api) {
		code = int(api.ErrorCode)
	}
	t.Logf("FINDING: after %d messages the publish ack returned: %v (api error code %d); errors.Is(ErrStreamFull)=%v",
		published, firstErr, code, errors.Is(firstErr, bus.ErrStreamFull))
	if !errors.Is(firstErr, bus.ErrStreamFull) {
		t.Fatalf("relay must see a clear, classified error, got %v", firstErr)
	}
	if n := streamMsgs(t, env, "EVT_IDENTITY"); n != uint64(published) {
		t.Fatalf("stream holds %d, published %d: old messages must not be discarded", n, published)
	}

	// The relay keeps the row and retries while the stream is full.
	db := env.NewDB(t)
	startRelay(t, env, db, pub, 7)
	commit(t, db, deviceRevoked(tenantA, "overflow", 1))
	time.Sleep(600 * time.Millisecond)
	n, oldest, err := outbox.Pending(ctx, db, time.Now())
	if err != nil || n != 1 {
		t.Fatalf("pending=%d err=%v; the row must stay in the outbox", n, err)
	}
	t.Logf("oldest unpublished row age %v (alert threshold 60s)", oldest)
}
