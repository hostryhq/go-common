//go:build integration

package bustest_test

import (
	"context"
	"testing"
	"time"

	"github.com/hostryhq/go-common/bus"
	"github.com/hostryhq/go-common/internal/bustest"
	"github.com/hostryhq/go-common/outbox"
	"github.com/hostryhq/go-common/restore"
	"github.com/hostryhq/go-common/restore/restoretest"
)

// RG2 (ADR-0002 A2.7): hostry-bus-reset with services running. The relay notices the new stream
// generation by itself and republishes the outbox inside the horizon; nothing restarts.
func TestRG2_BusResetWithRunningRelayRepublishesTheOutbox(t *testing.T) {
	env := bustest.Shared(t)
	env.ResetBus(t, bus.ProvisionOptions{Generation: "gen-1", Epoch: "genesis"})
	db := env.NewDB(t)
	nc := env.Connect(t, "identity") // the service's own credential: it may read its own stream info
	pub, _ := bus.NewPublisher(nc)
	r := &outbox.Relay{DB: db, Sink: pub, LockKey: 11, Poll: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
		Stream: "EVT_IDENTITY", Generation: bus.GenerationFunc(nc, "identity"), GenerationEvery: 100 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	commit(t, db, deviceRevoked(tenantA, "a", 1), deviceRevoked(tenantA, "b", 1), deviceRevoked(tenantA, "c", 1))
	eventually(t, "3 rows published", func() bool {
		return count(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`) == 3
	})
	eventually(t, "first generation recorded without a replay", func() bool {
		return count(t, db, `SELECT count(*) FROM outbox_relay_state WHERE stream='EVT_IDENTITY' AND generation='gen-1'`) == 1
	})
	if n := streamMsgs(t, env, "EVT_IDENTITY"); n != 3 {
		t.Fatalf("stream holds %d, want 3", n)
	}

	env.ResetBus(t, bus.ProvisionOptions{Generation: "gen-2", Epoch: "genesis"}) // streams deleted and re-provisioned, services keep running
	if n := streamMsgs(t, env, "EVT_IDENTITY"); n != 0 {
		t.Fatalf("a reset bus is empty, has %d", n)
	}
	start := time.Now()
	eventually(t, "outbox republished and generation recorded", func() bool {
		return count(t, db, `SELECT count(*) FROM outbox_relay_state WHERE stream='EVT_IDENTITY' AND generation='gen-2'`) == 1
	})
	if n := streamMsgs(t, env, "EVT_IDENTITY"); n != 3 {
		t.Fatalf("after the reset the stream must hold the 7-day outbox again: %d, want 3", n)
	}
	if time.Since(start) > time.Minute {
		t.Fatal("republish must happen within a minute")
	}
	if n := count(t, db, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("%d rows still unpublished", n)
	}
}

// The reader, restore_state and the guard on real Postgres: genesis, then a restore epoch.
func TestRestoreGuardOnPostgres(t *testing.T) {
	env := bustest.Shared(t)
	ctx := context.Background()
	platform := env.NewDB(t)
	svc := env.NewDB(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := restore.NewReader(env.DSN(platform)).Current(ctx); err == nil {
		t.Fatal("an unstamped platform_meta must refuse to give an epoch")
	}
	must(restoretest.Stamp(ctx, platform, "postgres", restore.Epoch{EpochID: restore.Genesis, Class: "genesis"}))

	jumps := 0
	run := func() (*restore.Guard, error) {
		g := restore.NewGuard(restore.NewReader(env.DSN(platform)), restore.NewSQLState(svc, true), nil)
		g.OnRestore("jump", func(context.Context, string, restore.Epoch) error { jumps++; return nil })
		return g, g.Run(ctx)
	}
	g, err := run()
	must(err)
	if g.Epoch().EpochID != restore.Genesis || jumps != 0 {
		t.Fatalf("first start records genesis and runs no hook: %+v jumps %d", g.Epoch(), jumps)
	}
	rt := time.Now().Add(-5 * time.Minute).UTC().Truncate(time.Millisecond)
	must(restoretest.Stamp(ctx, platform, "postgres", restore.Epoch{EpochID: "restore-1", RecoveredTo: rt, Class: "A"}))
	g, err = run()
	must(err)
	if e := g.Epoch(); e.EpochID != "restore-1" || !e.RecoveredTo.Equal(rt) || jumps != 1 {
		t.Fatalf("restore-1 must run the hook once: %+v jumps %d", e, jumps)
	}
	if _, err = run(); err != nil || jumps != 1 {
		t.Fatalf("a restart under the same epoch runs nothing: %v jumps %d", err, jumps)
	}
}
