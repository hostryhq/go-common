package sync

import (
	"crypto/sha1" //nolint:gosec // UUIDv5 is defined over SHA-1; not used for security.
	"encoding/hex"
)

var uuidNS = func() [16]byte {
	var ns [16]byte
	h := make([]byte, 0, 32)
	for _, c := range []byte(UUIDv5Namespace) {
		if c != '-' {
			h = append(h, c)
		}
	}
	if _, err := hex.Decode(ns[:], h); err != nil {
		panic(err)
	}
	return ns
}()

// UUIDv5 returns the deterministic row id for name under the Hostry namespace, so that two devices
// (or an emit repeated after a restore) converge on one row. Names are built by the caller from
// the entity's `id.name` template.
func UUIDv5(name string) string {
	h := sha1.New() //nolint:gosec
	h.Write(uuidNS[:])
	h.Write([]byte(name))
	sum := h.Sum(nil)
	var u [16]byte
	copy(u[:], sum[:16])
	u[6] = u[6]&0x0f | 0x50
	u[8] = u[8]&0x3f | 0x80
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], u[10:16])
	return string(b[:])
}
