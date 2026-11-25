package restore_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hostryhq/go-common/health"
	"github.com/hostryhq/go-common/restore"
)

type fakeSource struct{ ep restore.Epoch }

func (f fakeSource) Current(context.Context) (restore.Epoch, error) { return f.ep, nil }

// memState is a restore_state row that survives "process restarts" in a test.
type memState struct {
	id    string
	found bool
}

func (m *memState) Load(context.Context) (string, bool, error) { return m.id, m.found, nil }
func (m *memState) Save(_ context.Context, id string, _ time.Time) error {
	m.id, m.found = id, true
	return nil
}

// allocator stands in for tenancy's allocator row: a jump is applied only to a row whose
// jumped_epoch differs, which is what makes the hook exactly-once per epoch.
type allocator struct {
	jumpedEpoch string
	jumps       int
}

func (a *allocator) hook(_ context.Context, _ string, to restore.Epoch) error {
	if a.jumpedEpoch == to.EpochID {
		return nil
	}
	a.jumps++
	a.jumpedEpoch = to.EpochID
	return nil
}

// RG1: a service starts under a new epoch and the process dies between two hooks: the hooks
// complete exactly once per epoch, and the service is not ready until restore_state is written.
func TestRG1_KilledBetweenHooksCompletesEachHookOnceAndStaysNotReadyUntilStateIsWritten(t *testing.T) {
	ctx := context.Background()
	state := &memState{id: "e1", found: true}
	src := fakeSource{restore.Epoch{EpochID: "e2", Class: "A"}}
	alloc := &allocator{jumpedEpoch: "e1"}
	var second int
	crash := true

	build := func() *restore.Guard {
		g := restore.NewGuard(src, state, nil)
		g.OnRestore("allocator", alloc.hook)
		g.OnRestore("second", func(context.Context, string, restore.Epoch) error {
			if crash {
				return errors.New("killed")
			}
			second++
			return nil
		})
		return g
	}

	g := build()
	if err := g.Run(ctx); err == nil {
		t.Fatal("a failing hook must fail Run")
	}
	if g.Ready() || g.Check(ctx) == nil {
		t.Fatal("not ready while a hook failed")
	}
	if state.id != "e1" {
		t.Fatalf("restore_state must not move before every hook finished, got %s", state.id)
	}

	// The next process start: same database, hooks run again.
	crash = false
	g = build()
	if err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !g.Ready() || g.Check(ctx) != nil || state.id != "e2" {
		t.Fatalf("ready after state written: ready=%v state=%s", g.Ready(), state.id)
	}
	if alloc.jumps != 1 || second != 1 {
		t.Fatalf("each hook must take effect once per epoch: allocator %d, second %d", alloc.jumps, second)
	}

	// A third start under the same epoch runs nothing.
	g = build()
	if err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if alloc.jumps != 1 || second != 1 {
		t.Fatal("no hook runs when the epoch is unchanged")
	}
}

func TestGuard_NewDatabaseRecordsTheEpochWithoutRunningHooks(t *testing.T) {
	state := &memState{}
	g := restore.NewGuard(fakeSource{restore.Epoch{EpochID: "e9"}}, state, nil)
	g.OnRestore("never", func(context.Context, string, restore.Epoch) error {
		t.Fatal("a new database has nothing to repair")
		return nil
	})
	if err := g.Run(context.Background()); err != nil || state.id != "e9" || g.Epoch().EpochID != "e9" {
		t.Fatalf("err=%v state=%s", err, state.id)
	}
}

func TestGuard_ReadinessCheckFailsUntilDone(t *testing.T) {
	g := restore.NewGuard(fakeSource{restore.Epoch{EpochID: "e1"}}, &memState{}, nil)
	h := health.New(time.Second)
	h.Add("restore", g.Check)
	ready := func() int {
		rec := httptest.NewRecorder()
		h.Ready(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return rec.Code
	}
	if ready() != http.StatusServiceUnavailable {
		t.Fatal("/readyz must fail before the guard has run")
	}
	select {
	case <-g.Done():
		t.Fatal("Done must stay open before Run")
	default:
	}
	if err := g.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-g.Done()
	if ready() != http.StatusOK {
		t.Fatal("/readyz ok after Run")
	}
}
