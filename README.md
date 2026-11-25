# go-common

Shared Go libraries for Hostry services (module `github.com/hostryhq/go-common`, Go 1.27, standard library only).

| Package | Purpose |
|---|---|
| `ids` | UUIDv7 generator (monotonic, time-sortable, client-side safe) |
| `money` | int64 paise `Money`, overflow-safe `Bps`/`MulRatio`, exact `Split` |
| `clock` | `Clock` interface, system clock, IST display helper |
| `apperr` | Typed errors mapped to RFC 9457 `application/problem+json` |
| `config` | Env-only typed loader that collects all errors |
| `logx` | `slog` JSON logger, request/tenant correlation, key-based redaction |
| `tenancy` | Tenant context, HTTP middleware (extractor supplied by go-auth), `set_config('app.tenant_id')` transaction helper |
| `idempotency` | `Idempotency-Key` parsing, fingerprint, `Store` interface, in-memory store |
| `events` | ADR-0011 event envelope, subjects, validation |
| `health` | `/healthz` and `/readyz` |
| `httpx` | Request-id and panic-recovery middleware |
| `testkit` | Fake clock, deterministic ids, two test tenants, HTTP helpers |
| `restore` | Restore epoch (ADR-0002 A2.4): `Reader` (read once at start, read-only), per-service `restore_state` (`StateSchema`), `Guard` with `OnRestore` hooks, `Check` for `/readyz`; `restore/restoretest.Stamp` for dev and CI |
| `outbox` | Transactional outbox and relay; the relay checks the bus generation at connect and every minute (`Relay.Generation`, `RelayStateSchema`, ADR-0011 Note N1) |

Run tests: `go vet ./... && go test -race ./...`; the bus, outbox and restore integration tests: `go test -tags integration ./internal/bustest` (needs Docker; starts its own compose project on free ports and removes it).

Startup order in a service: read the epoch (`restore.Guard.Run`), register hooks first with `OnRestore`, add `guard.Check` to `/readyz`, start the relay only after `guard.Done()`.

Not here yet (see ADRs): `sync` (HLC, op store, ADR-0002), RLS migration helper, shared isolation suites T1-T5, metrics endpoint.
