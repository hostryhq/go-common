package sync

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
)

// Domain tags of the signed and hashed messages (protocol-v1.md). A change to any of them takes a
// new tag, never a redefinition.
const (
	GenesisTag  = "hostry-genesis-v1"
	SealTag     = "hostry-seal-v1"
	EndorseTag  = "hostry-endorse-v1"
	AttestTag   = "hostry-attest-v1"
	HLCHexWidth = 16
)

// Hash is the lowercase hex SHA-256 of the UTF-8 bytes of body: the h of an op.
func Hash(body string) string {
	h := sha256.Sum256([]byte(body))
	return hex.EncodeToString(h[:])
}

// Genesis is the ph of the first op of (origin, stream).
func Genesis(origin, stream string) string { return Hash(GenesisTag + "\n" + origin + "\n" + stream) }

// SealMessage is what an origin or the gateway signs at a seal boundary.
func SealMessage(origin, stream string, seq int64, h string) string {
	return SealTag + "\n" + origin + "\n" + stream + "\n" + strconv.FormatInt(seq, 10) + "\n" + h
}

// EndorseMessage is what the gateway signs to endorse a released op of a key-lost origin.
func EndorseMessage(origin, stream string, seq int64, h, rec string) string {
	return EndorseTag + "\n" + origin + "\n" + stream + "\n" + strconv.FormatInt(seq, 10) + "\n" + h + "\n" + rec
}

// AttestMessage is what an adopting device signs to fix the tail of a key-lost origin.
func AttestMessage(origin, stream string, seq int64, h, by, rec string) string {
	return AttestTag + "\n" + origin + "\n" + stream + "\n" + strconv.FormatInt(seq, 10) + "\n" + h + "\n" + by + "\n" + rec
}

// VerifyES256 checks an ES256 signature (raw r||s, base64url) over msg under an X9.62 uncompressed
// P-256 public key (base64url). Any malformed input verifies as false.
func VerifyES256(pubB64URL, msg, sigB64URL string) bool {
	pb, err := base64.RawURLEncoding.DecodeString(pubB64URL)
	if err != nil {
		return false
	}
	pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), pb)
	if err != nil {
		return false
	}
	sb, err := base64.RawURLEncoding.DecodeString(sigB64URL)
	if err != nil || len(sb) != 64 {
		return false
	}
	der, err := asn1.Marshal(struct{ R, S *big.Int }{new(big.Int).SetBytes(sb[:32]), new(big.Int).SetBytes(sb[32:])})
	if err != nil {
		return false
	}
	h := sha256.Sum256([]byte(msg))
	return ecdsa.VerifyASN1(pub, h[:], der)
}

// VerifyEndorsement checks an endorsement signature under a current op-signing key.
func VerifyEndorsement(pub, origin, stream string, seq int64, h, rec, sig string) bool {
	return VerifyES256(pub, EndorseMessage(origin, stream, seq, h, rec), sig)
}

// HLC is a hybrid logical clock stamp: 48-bit milliseconds and a 16-bit counter, written as 16
// lowercase hex characters so that string order is time order.
type HLC struct {
	Ms      int64
	Counter uint16
}

// String is the 16-character hex form.
func (c HLC) String() string { return fmt.Sprintf("%012x%04x", c.Ms, c.Counter) }

// ParseHLC parses the 16-character lowercase hex form.
func ParseHLC(s string) (HLC, error) {
	if len(s) != HLCHexWidth {
		return HLC{}, fmt.Errorf("sync: hlc %q is not 16 hex characters", s)
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return HLC{}, fmt.Errorf("sync: hlc %q is not lowercase hex", s)
		}
	}
	ms, _ := strconv.ParseInt(s[:12], 16, 64)
	ctr, _ := strconv.ParseUint(s[12:], 16, 16)
	return HLC{Ms: ms, Counter: uint16(ctr)}, nil
}
