package sync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
)

// Schema is the forward-only migration for the Postgres Store and the erasable pii side table.
// Services copy it into repository/migrations. sync_candidates takes inserts only, with
// ON CONFLICT DO NOTHING, so concurrent consumers never block each other and redelivery is a no-op.
// sync_pii holds the personal data of entities the service owns, keyed so that the erasure of one
// row (sync.pii.erased) is a delete; it never holds a replica's data and never lives in op-log partitions.
const Schema = `
CREATE TABLE IF NOT EXISTS sync_candidates (
  tenant_id text   NOT NULL,
  entity    text   NOT NULL,
  row_id    text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  idx       int    NOT NULL,
  hlc       text   NOT NULL,
  kind      text   NOT NULL,
  fields    jsonb  NOT NULL DEFAULT '{}',
  prev      jsonb  NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, entity, row_id, origin, stream, seq, idx)
);
CREATE INDEX IF NOT EXISTS sync_candidates_op ON sync_candidates (tenant_id, origin, stream, seq);
CREATE TABLE IF NOT EXISTS sync_voids (
  tenant_id text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  PRIMARY KEY (tenant_id, origin, stream, seq)
);
CREATE TABLE IF NOT EXISTS sync_pii (
  tenant_id text   NOT NULL,
  entity    text   NOT NULL,
  row_id    text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  fields    jsonb  NOT NULL,
  salts     jsonb  NOT NULL DEFAULT '{}',
  commits   jsonb  NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, entity, row_id, origin, stream, seq)
);
ALTER TABLE sync_pii ADD COLUMN IF NOT EXISTS salts   jsonb NOT NULL DEFAULT '{}';
ALTER TABLE sync_pii ADD COLUMN IF NOT EXISTS commits jsonb NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS sync_pii_erased (
  tenant_id text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  PRIMARY KEY (tenant_id, origin, stream, seq)
);
`

// DBTX is the part of *sql.DB and *sql.Tx the Postgres store needs. Pass the consumer's tx so the
// projection commits with the inbox record.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// PGStore is the Postgres Store. Every statement filters by tenant_id.
type PGStore struct{ DB DBTX }

var _ Store = PGStore{}

// AddCandidate implements Store.
func (s PGStore) AddCandidate(ctx context.Context, k RowKey, c Candidate) error {
	f, err := json.Marshal(nonNil(c.F))
	if err != nil {
		return err
	}
	p, err := json.Marshal(nonNilS(c.Prev))
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx,
		`INSERT INTO sync_candidates (tenant_id, entity, row_id, origin, stream, seq, idx, hlc, kind, fields, prev)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING`,
		k.Tenant, k.Entity, k.ID, c.Ref.Origin, c.Ref.Stream, c.Ref.Seq, c.Idx, c.HLC, c.Kind, string(f), string(p))
	return err
}

// Candidates implements Store.
func (s PGStore) Candidates(ctx context.Context, k RowKey) ([]Candidate, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT origin, stream, seq, idx, hlc, kind, fields, prev FROM sync_candidates WHERE tenant_id=$1 AND entity=$2 AND row_id=$3`,
		k.Tenant, k.Entity, k.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var c Candidate
		var f, p []byte
		if err := rows.Scan(&c.Ref.Origin, &c.Ref.Stream, &c.Ref.Seq, &c.Idx, &c.HLC, &c.Kind, &f, &p); err != nil {
			return nil, err
		}
		if c.F, err = decodeFields(f); err != nil {
			return nil, err
		}
		if len(c.F) == 0 {
			c.F = nil
		}
		if err := json.Unmarshal(p, &c.Prev); err != nil {
			return nil, err
		}
		if len(c.Prev) == 0 {
			c.Prev = nil
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func decodeFields(b []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	for k, v := range raw {
		if n, ok := v.(json.Number); ok {
			i, err := parseSafeInt(n.String())
			if err != nil {
				return nil, err
			}
			raw[k] = i
		}
	}
	return raw, nil
}

// RowsOfOp implements Store.
func (s PGStore) RowsOfOp(ctx context.Context, tenant string, r OpRef) ([]RowKey, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT DISTINCT entity, row_id FROM sync_candidates WHERE tenant_id=$1 AND origin=$2 AND stream=$3 AND seq=$4`,
		tenant, r.Origin, r.Stream, r.Seq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RowKey
	for rows.Next() {
		k := RowKey{Tenant: tenant}
		if err := rows.Scan(&k.Entity, &k.ID); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// AddVoid implements Store.
func (s PGStore) AddVoid(ctx context.Context, tenant string, r OpRef) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO sync_voids (tenant_id, origin, stream, seq) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
		tenant, r.Origin, r.Stream, r.Seq)
	return err
}

// Voided implements Store. It asks per distinct ref; a row has few candidates.
func (s PGStore) Voided(ctx context.Context, tenant string, refs []OpRef) (map[OpRef]bool, error) {
	out := map[OpRef]bool{}
	seen := map[OpRef]bool{}
	for _, r := range refs {
		if seen[r] {
			continue
		}
		seen[r] = true
		rows, err := s.DB.QueryContext(ctx, `SELECT 1 FROM sync_voids WHERE tenant_id=$1 AND origin=$2 AND stream=$3 AND seq=$4`,
			tenant, r.Origin, r.Stream, r.Seq)
		if err != nil {
			return nil, err
		}
		if rows.Next() {
			out[r] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

// PutPII stores the personal data an Apply returned (owned entities only). Idempotent per op.
func (s PGStore) PutPII(ctx context.Context, w PIIWrite) error {
	f, err := json.Marshal(w.Fields)
	if err != nil {
		return err
	}
	sl, err := json.Marshal(nonNilS(w.Salts))
	if err != nil {
		return err
	}
	cm, err := json.Marshal(nonNilS(w.Commits))
	if err != nil {
		return err
	}
	// An op whose pii was erased never gets its values back, whatever the delivery order.
	_, err = s.DB.ExecContext(ctx,
		`INSERT INTO sync_pii (tenant_id, entity, row_id, origin, stream, seq, fields, salts, commits)
		 SELECT $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb
		 WHERE NOT EXISTS (SELECT 1 FROM sync_pii_erased WHERE tenant_id=$1 AND origin=$4 AND stream=$5 AND seq=$6)
		 ON CONFLICT DO NOTHING`,
		w.Key.Tenant, w.Key.Entity, w.Key.ID, w.Ref.Origin, w.Ref.Stream, w.Ref.Seq, string(f), string(sl), string(cm))
	return err
}

// AddErasure implements Store: it records the erasure and empties the values and salts of the op's
// side-table records, keeping the commitments.
func (s PGStore) AddErasure(ctx context.Context, tenant string, r OpRef) (int64, error) {
	if _, err := s.DB.ExecContext(ctx,
		`INSERT INTO sync_pii_erased (tenant_id, origin, stream, seq) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
		tenant, r.Origin, r.Stream, r.Seq); err != nil {
		return 0, err
	}
	res, err := s.DB.ExecContext(ctx,
		`UPDATE sync_pii SET fields='{}', salts='{}' WHERE tenant_id=$1 AND origin=$2 AND stream=$3 AND seq=$4 AND fields <> '{}'::jsonb`,
		tenant, r.Origin, r.Stream, r.Seq)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Erased implements Store.
func (s PGStore) Erased(ctx context.Context, tenant string, r OpRef) (bool, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT 1 FROM sync_pii_erased WHERE tenant_id=$1 AND origin=$2 AND stream=$3 AND seq=$4`, tenant, r.Origin, r.Stream, r.Seq)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	found := rows.Next()
	return found, rows.Err()
}

// ErasePII deletes every personal-data record of one row (the reaction to sync.pii.erased). It
// returns the number of records removed.
func (s PGStore) ErasePII(ctx context.Context, k RowKey) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `DELETE FROM sync_pii WHERE tenant_id=$1 AND entity=$2 AND row_id=$3`, k.Tenant, k.Entity, k.ID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func nonNil(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func nonNilS(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
