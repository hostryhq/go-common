package sync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
)

// Canonicalize returns the RFC 8785 canonical form of the integer-only subset: keys sorted by
// UTF-16 code units, no whitespace, minimal string escapes, integers within +-(2^53-1), "-0" as
// "0". Fractions, exponents, duplicate keys and trailing data are refused (ErrNotCanonical).
func Canonicalize(in []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	var out bytes.Buffer
	if err := canonValue(dec, &out); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotCanonical, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data", ErrNotCanonical)
	}
	return out.Bytes(), nil
}

// IsCanonical reports whether b is already in canonical form.
func IsCanonical(b []byte) bool {
	c, err := Canonicalize(b)
	return err == nil && bytes.Equal(c, b)
}

func canonValue(dec *json.Decoder, out *bytes.Buffer) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	return canonToken(dec, tok, out)
}

func canonToken(dec *json.Decoder, tok json.Token, out *bytes.Buffer) error {
	switch t := tok.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(t))
	case string:
		writeCanonString(out, t)
	case json.Number:
		n, err := parseSafeInt(t.String())
		if err != nil {
			return err
		}
		out.WriteString(strconv.FormatInt(n, 10))
	case json.Delim:
		if t == '[' {
			out.WriteByte('[')
			first := true
			for dec.More() {
				if !first {
					out.WriteByte(',')
				}
				first = false
				if err := canonValue(dec, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			out.WriteByte(']')
			return nil
		}
		type kv struct {
			k string
			v []byte
		}
		var members []kv
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			k, ok := kt.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if seen[k] {
				return fmt.Errorf("duplicate key %q", k)
			}
			seen[k] = true
			var vb bytes.Buffer
			if err := canonValue(dec, &vb); err != nil {
				return err
			}
			members = append(members, kv{k, vb.Bytes()})
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
		sort.Slice(members, func(i, j int) bool { return utf16Less(members[i].k, members[j].k) })
		out.WriteByte('{')
		for i, m := range members {
			if i > 0 {
				out.WriteByte(',')
			}
			writeCanonString(out, m.k)
			out.WriteByte(':')
			out.Write(m.v)
		}
		out.WriteByte('}')
	default:
		return fmt.Errorf("unexpected token %T", tok)
	}
	return nil
}

// parseSafeInt accepts -?(0|[1-9][0-9]*) within +-(2^53-1); anything else is refused.
func parseSafeInt(s string) (int64, error) {
	if strings.ContainsAny(s, ".eE+") {
		return 0, fmt.Errorf("fraction or exponent %q", s)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("integer %q out of range", s)
	}
	if n > MaxSafeInt || n < -MaxSafeInt {
		return 0, fmt.Errorf("integer %q beyond 2^53-1", s)
	}
	return n, nil
}

func utf16Less(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}

func writeCanonString(out *bytes.Buffer, s string) {
	out.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			out.WriteString(`\"`)
		case r == '\\':
			out.WriteString(`\\`)
		case r == '\b':
			out.WriteString(`\b`)
		case r == '\f':
			out.WriteString(`\f`)
		case r == '\n':
			out.WriteString(`\n`)
		case r == '\r':
			out.WriteString(`\r`)
		case r == '\t':
			out.WriteString(`\t`)
		case r < 0x20:
			fmt.Fprintf(out, `\u%04x`, r)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
}
