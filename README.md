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
| `idempotency` | `Idempotency-Key` parsing, fingerprint, `Store` interface, in-memory store; Postgres `DoTx` (own tx) and `DoTxIn` (caller-supplied tx, savepoint-protected) |
| `events` | ADR-0011 event envelope, subjects, validation |
| `health` | `/healthz` and `/readyz` |
| `httpx` | Request-id and panic-recovery middleware |
| `testkit` | Fake clock, deterministic ids, two test tenants, HTTP helpers |
| `restore` | Restore epoch (ADR-0002 A2.4): `Reader` (read once at start, read-only), per-service `restore_state` (`StateSchema`), `Guard` with `OnRestore` hooks, `Check` for `/readyz`; `restore/restoretest.Stamp` for dev and CI |
| `outbox` | Transactional outbox and relay; the relay checks the bus generation at connect and every minute (`Relay.Generation`, `RelayStateSchema`, ADR-0011 Note N1) |
| `sync` | Sync protocol v1 (ADR-0002, ADR-0014 A2): `ParseOp` (the one parser of `sync.op.*` events; size, hash and consistency checks, upcasters), canonical JSON, hashes, signed-message builders, HLC, UUIDv5; entity `Registry` loaded from the api-specs entity YAML; projection `Engine` (candidates per mutation, `Resolve` per field policy, voids, order-free and idempotent), `MemStore` and `PGStore` (`Schema`, erasable pii side table) |
| `emit` | Client of the gateway `POST /internal/v1/emit` (service token, per-op idempotency key, 503 `emit.unavailable` with Retry-After) and `emit_queue` (`QueueSchema`, `Enqueue` in the caller's tx, `Queue.Drain`/`Run` with backoff, ordered per tenant) |

Run tests: `go vet ./... && go test -race ./...`; the bus, outbox, restore, sync projection store and emit queue integration tests: `go test -tags integration ./internal/bustest` (needs Docker; starts its own compose project on free ports and removes it).

Startup order in a service: read the epoch (`restore.Guard.Run`), register hooks first with `OnRestore`, add `guard.Check` to `/readyz`, start the relay only after `guard.Done()`.

Not here yet (see ADRs): RLS migration helper, shared isolation suites T1-T5, metrics endpoint.
