package sync

// Salted per-field commitments for personal data beside an op (ADR-0014 A2.1; PROVISIONAL, security
// ruling of the Phase 1 re-review). Mirrors svc-sync syncproto/piic.go, which is authoritative:
//
//	body.pii_c[entity][row id][field] = hex SHA-256( salt || JCS(value) )
//
// salt is 16 random bytes. On the wire the salt of a field travels in the same pii object under
// the key "~s:<field>" (base64url, no padding). A field whose commitment, salt or value does not
// check out is dropped, never kept.

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// PIIC is entity -> row id -> field -> commitment (hex), the content of body.pii_c.
type PIIC map[string]map[string]map[string]string

const (
	piiSaltPrefix = "~s:"
	piiSaltBytes  = 16
)

// PIICommit returns hex SHA-256(salt || JCS(value)) for a string, bool or int64 value.
func PIICommit(salt []byte, value any) (string, error) {
	c, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write(salt)
	h.Write(c)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// verifiedPII is the result of checking wire pii against body.pii_c.
type verifiedPII struct {
	values  map[string]map[string]map[string]any
	salts   map[string]map[string]map[string]string // base64url, as sent
	commits PIIC                                    // only for the fields that verified
	dropped int
}

func verifyPII(raw []byte, c PIIC) (verifiedPII, error) {
	var v verifiedPII
	var in map[string]map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &in); err != nil {
		return v, malformed("pii must be entity -> row id -> field -> value: %v", err)
	}
	for e, rows := range in {
		for id, fields := range rows {
			for name, rv := range fields {
				if strings.HasPrefix(name, piiSaltPrefix) {
					continue
				}
				val, err := decodePIIValue(rv)
				if err != nil {
					return v, malformed("pii %s field %q: %v", e, name, err)
				}
				want, ok := c[e][id][name]
				var saltS string
				saltRaw, hasSalt := fields[piiSaltPrefix+name]
				if hasSalt {
					_ = json.Unmarshal(saltRaw, &saltS)
				}
				salt, derr := base64.RawURLEncoding.DecodeString(saltS)
				if !ok || !hasSalt || derr != nil || len(salt) != piiSaltBytes {
					v.dropped++
					continue
				}
				got, err := PIICommit(salt, val)
				if err != nil || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
					v.dropped++
					continue
				}
				if v.values == nil {
					v.values, v.salts, v.commits = map[string]map[string]map[string]any{}, map[string]map[string]map[string]string{}, PIIC{}
				}
				if v.values[e] == nil {
					v.values[e], v.salts[e], v.commits[e] = map[string]map[string]any{}, map[string]map[string]string{}, map[string]map[string]string{}
				}
				if v.values[e][id] == nil {
					v.values[e][id], v.salts[e][id], v.commits[e][id] = map[string]any{}, map[string]string{}, map[string]string{}
				}
				v.values[e][id][name], v.salts[e][id][name], v.commits[e][id][name] = val, saltS, want
			}
		}
	}
	return v, nil
}

// CanonicalJSON marshals v and returns its RFC 8785 form (integer-only subset).
func CanonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Canonicalize(b)
}

func decodePIIValue(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case bool, string:
		return t, nil
	case json.Number:
		return parseSafeInt(t.String())
	}
	return nil, fmt.Errorf("pii values are strings, booleans or integers")
}
