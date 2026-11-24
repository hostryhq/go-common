package tenancy

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hostryhq/go-common/ids"
)

func TestMiddlewareFailsClosedWithoutTenant(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })
	h := Middleware(func(*http.Request) (Context, error) { return Context{}, nil })(next)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if called || rec.Code != http.StatusUnauthorized {
		t.Fatalf("called=%v status=%d", called, rec.Code)
	}
}

func TestMiddlewareSetsContext(t *testing.T) {
	want := Context{TenantID: ids.New(), OutletID: ids.New(), ActorID: "m1"}
	var got Context
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got, _ = From(r.Context()) })
	h := Middleware(func(*http.Request) (Context, error) { return want, nil })(next)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

type fakeExec struct {
	query string
	args  []any
}

func (f *fakeExec) ExecContext(_ context.Context, q string, a ...any) (sql.Result, error) {
	f.query, f.args = q, a
	return nil, nil
}

func TestSetDBContextIsTransactionLocalAndRejectsEmpty(t *testing.T) {
	f := &fakeExec{}
	id := ids.New()
	if err := SetDBContext(context.Background(), f, id); err != nil {
		t.Fatal(err)
	}
	if f.query != "SELECT set_config('app.tenant_id', $1, true)" || f.args[0] != id.String() {
		t.Fatalf("query=%q args=%v", f.query, f.args)
	}
	if err := SetDBContext(context.Background(), f, ""); err == nil {
		t.Fatal("empty tenant accepted")
	}
}

func TestRequireWithoutTenant(t *testing.T) {
	if _, err := Require(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}
