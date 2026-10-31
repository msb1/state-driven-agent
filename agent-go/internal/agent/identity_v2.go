package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// StrictJSON rejects duplicate members before request normalization or admission.
func StrictJSON(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("invalid UTF-8")
	}
	// Reject unpaired escaped surrogates; encoding/json otherwise substitutes U+FFFD.
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		i++
		for i < len(data) && data[i] != '"' {
			if data[i] == '\\' {
				i++
				if i >= len(data) {
					break
				}
				if data[i] == 'u' && i+4 < len(data) {
					n, e := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
					if e != nil {
						return nil, e
					}
					i += 4
					if n >= 0xd800 && n <= 0xdbff {
						if i+6 >= len(data) || string(data[i+1:i+3]) != "\\u" {
							return nil, fmt.Errorf("unpaired surrogate")
						}
						m, e := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
						if e != nil || m < 0xdc00 || m > 0xdfff {
							return nil, fmt.Errorf("unpaired surrogate")
						}
						i += 6
					} else if n >= 0xdc00 && n <= 0xdfff {
						return nil, fmt.Errorf("unpaired surrogate")
					}
				}
			}
			i++
		}
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var read func() (any, error)
	read = func() (any, error) {
		t, e := d.Token()
		if e != nil {
			return nil, e
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				m := map[string]any{}
				for d.More() {
					k, e := d.Token()
					if e != nil {
						return nil, e
					}
					key := k.(string)
					if _, ok := m[key]; ok {
						return nil, fmt.Errorf("duplicate JSON property")
					}
					v, e := read()
					if e != nil {
						return nil, e
					}
					m[key] = v
				}
				_, e = d.Token()
				return m, e
			case '[':
				a := []any{}
				for d.More() {
					v, e := read()
					if e != nil {
						return nil, e
					}
					a = append(a, v)
				}
				_, e = d.Token()
				return a, e
			}
		}
		return t, nil
	}
	v, e := read()
	if e != nil {
		return nil, e
	}
	if _, e = d.Token(); e != io.EOF {
		return nil, fmt.Errorf("extra JSON content")
	}
	_, e = CanonicalV2(v)
	return v, e
}

// CanonicalV2 implements the contract's JCS UTF-16 property ordering and binary64 numbers.
func CanonicalV2(value any) ([]byte, error) {
	raw, e := json.Marshal(value)
	if e != nil {
		return nil, e
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if e = d.Decode(&v); e != nil {
		return nil, e
	}
	var b bytes.Buffer
	var write func(any) error
	quote := func(s string) {
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(false)
		var tmp bytes.Buffer
		enc = json.NewEncoder(&tmp)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(s)
		x := bytes.TrimSuffix(tmp.Bytes(), []byte("\n"))
		x = bytes.ReplaceAll(x, []byte(`\u2028`), []byte("\u2028"))
		x = bytes.ReplaceAll(x, []byte(`\u2029`), []byte("\u2029"))
		b.Write(x)
	}
	write = func(v any) error {
		switch x := v.(type) {
		case nil:
			b.WriteString("null")
		case bool:
			if x {
				b.WriteString("true")
			} else {
				b.WriteString("false")
			}
		case string:
			quote(x)
		case json.Number:
			if !strings.ContainsAny(string(x), ".eE") {
				i, e := strconv.ParseInt(string(x), 10, 64)
				if e != nil || i > 9007199254740991 || i < -9007199254740991 {
					return fmt.Errorf("integer outside I-JSON range")
				}
			}
			f, e := strconv.ParseFloat(string(x), 64)
			if e != nil || math.IsInf(f, 0) || math.IsNaN(f) {
				return fmt.Errorf("unsupported JSON number")
			}
			if f == 0 {
				b.WriteByte('0')
				break
			}
			a := math.Abs(f)
			format := byte('e')
			if a >= 1e-6 && a < 1e21 {
				format = 'f'
			}
			s := strconv.FormatFloat(f, format, -1, 64)
			s = strings.Replace(s, "e-0", "e-", 1)
			s = strings.Replace(s, "e+0", "e+", 1)
			b.WriteString(s)
		case []any:
			b.WriteByte('[')
			for i, v := range x {
				if i > 0 {
					b.WriteByte(',')
				}
				if e := write(v); e != nil {
					return e
				}
			}
			b.WriteByte(']')
		case map[string]any:
			keys := []string{}
			for k := range x {
				keys = append(keys, k)
			}
			sort.Slice(keys, func(i, j int) bool {
				a, c := utf16.Encode([]rune(keys[i])), utf16.Encode([]rune(keys[j]))
				for n := 0; n < len(a) && n < len(c); n++ {
					if a[n] != c[n] {
						return a[n] < c[n]
					}
				}
				return len(a) < len(c)
			})
			b.WriteByte('{')
			for i, k := range keys {
				if i > 0 {
					b.WriteByte(',')
				}
				quote(k)
				b.WriteByte(':')
				if e := write(x[k]); e != nil {
					return e
				}
			}
			b.WriteByte('}')
		default:
			return fmt.Errorf("unsupported JSON value")
		}
		return nil
	}
	if e = write(v); e != nil {
		return nil, e
	}
	return b.Bytes(), nil
}
func DigestV2(value any) (string, error) {
	b, e := CanonicalV2(value)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:]), nil
}
