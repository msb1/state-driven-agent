package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
)

// strictJSON rejects duplicate object keys and numbers that cannot be represented
// as finite Go JSON numbers. Decoder.UseNumber preserves integer-vs-float input.
func strictJSON(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	v, err := readJSONValue(d)
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err == nil {
		return nil, fmt.Errorf("unexpected trailing JSON value")
	} else if err != io.EOF {
		return nil, err
	}
	return v, nil
}
func readJSONValue(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch x := t.(type) {
	case json.Delim:
		switch x {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				key, ok := k.(string)
				if !ok {
					return nil, fmt.Errorf("object key must be string")
				}
				if _, ok = m[key]; ok {
					return nil, fmt.Errorf("duplicate JSON property: %s", key)
				}
				v, err := readJSONValue(d)
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			_, err = d.Token()
			return m, err
		case '[':
			a := []any{}
			for d.More() {
				v, err := readJSONValue(d)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			_, err = d.Token()
			return a, err
		default:
			return nil, fmt.Errorf("unexpected JSON delimiter")
		}
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		if !strings.ContainsAny(x.String(), ".eE") {
			return nil, fmt.Errorf("JSON integer exceeds supported range")
		}
		f, err := x.Float64()
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, fmt.Errorf("JSON number exceeds finite numeric range")
		}
		return f, nil
	default:
		return t, nil
	}
}

func validateArguments(value any, schema map[string]any, path string) []string {
	if path == "" {
		path = "$"
	}
	kind, _ := schema["type"].(string)
	if kind != "" && !schemaType(value, kind) {
		return []string{fmt.Sprintf("%s: expected %s, received %T; supply a JSON %s without coercion", path, kind, value, kind)}
	}
	errs := []string{}
	if enum, ok := schema["enum"].([]any); ok {
		found := false
		for _, x := range enum {
			if jsonEqual(value, x) {
				found = true
				break
			}
		}
		if !found {
			errs = append(errs, fmt.Sprintf("%s: value is not in configured enum", path))
		}
	}
	if x, ok := schema["const"]; ok && !jsonEqual(value, x) {
		errs = append(errs, fmt.Sprintf("%s: value does not match configured constant", path))
	}
	switch v := value.(type) {
	case map[string]any:
		required, _ := schema["required"].([]any)
		for _, r := range required {
			if key, ok := r.(string); ok {
				if _, found := v[key]; !found {
					errs = append(errs, path+"."+key+": required property is missing")
				}
			}
		}
		props, _ := schema["properties"].(map[string]any)
		for k, item := range v {
			if s, ok := props[k].(map[string]any); ok {
				errs = append(errs, validateArguments(item, s, path+"."+k)...)
			} else if schema["additionalProperties"] == false {
				errs = append(errs, path+"."+k+": additional property is forbidden")
			} else if s, ok := schema["additionalProperties"].(map[string]any); ok {
				errs = append(errs, validateArguments(item, s, path+"."+k)...)
			}
		}
	case string:
		n := len([]rune(v))
		if min, ok := schema["minLength"].(int); ok && n < min {
			errs = append(errs, path+": string length is below configured minimum")
		}
		if max, ok := schema["maxLength"].(int); ok && n > max {
			errs = append(errs, path+": string length exceeds configured maximum")
		}
		if pattern, ok := schema["pattern"].(string); ok {
			if re, e := regexp.Compile(pattern); e == nil && !re.MatchString(v) {
				errs = append(errs, path+": string does not match configured pattern")
			}
		}
	case []any:
		if min, ok := schema["minItems"].(int); ok && len(v) < min {
			errs = append(errs, path+": array is shorter than configured minimum")
		}
		if max, ok := schema["maxItems"].(int); ok && len(v) > max {
			errs = append(errs, path+": array exceeds configured maximum")
		}
		if s, ok := schema["items"].(map[string]any); ok {
			for i, item := range v {
				errs = append(errs, validateArguments(item, s, fmt.Sprintf("%s[%d]", path, i))...)
			}
		}
	}
	if n, ok := schemaNumber(value); ok {
		for _, rule := range []struct {
			k     string
			valid func(float64) bool
		}{{"minimum", func(x float64) bool { return n >= x }}, {"maximum", func(x float64) bool { return n <= x }}, {"exclusiveMinimum", func(x float64) bool { return n > x }}, {"exclusiveMaximum", func(x float64) bool { return n < x }}} {
			if limit, ok := schemaNumber(schema[rule.k]); ok && !rule.valid(limit) {
				errs = append(errs, fmt.Sprintf("%s: violates %s=%v", path, rule.k, limit))
			}
		}
	}
	return errs
}
func schemaNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case json.Number:
		f, err := n.Float64()
		return f, err == nil && !math.IsInf(f, 0) && !math.IsNaN(f)
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, !math.IsInf(n, 0) && !math.IsNaN(n)
	}
	return 0, false
}
func schemaType(v any, kind string) bool {
	switch kind {
	case "object":
		_, ok := v.(map[string]any)
		return ok
	case "array":
		_, ok := v.([]any)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "null":
		return v == nil
	case "number":
		_, ok := schemaNumber(v)
		return ok
	case "integer":
		n, ok := schemaNumber(v)
		return ok && math.Trunc(n) == n
	}
	return false
}
func jsonEqual(a, b any) bool {
	if _, ok := a.(bool); ok {
		return a == b
	}
	if _, ok := b.(bool); ok {
		return a == b
	}
	if am, ok := a.(map[string]any); ok {
		bm, ok := b.(map[string]any)
		if !ok || len(am) != len(bm) {
			return false
		}
		for k, v := range am {
			w, found := bm[k]
			if !found || !jsonEqual(v, w) {
				return false
			}
		}
		return true
	}
	if aa, ok := a.([]any); ok {
		bb, ok := b.([]any)
		if !ok || len(aa) != len(bb) {
			return false
		}
		for i := range aa {
			if !jsonEqual(aa[i], bb[i]) {
				return false
			}
		}
		return true
	}
	if x, ok := schemaNumber(a); ok {
		y, yok := schemaNumber(b)
		return yok && x == y
	}
	return a == b
}

func checkSchemaValue(schema map[string]any) error {
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "additionalProperties": true, "items": true, "enum": true, "const": true, "minimum": true, "maximum": true, "exclusiveMinimum": true, "exclusiveMaximum": true, "minLength": true, "maxLength": true, "pattern": true, "minItems": true, "maxItems": true, "description": true, "title": true, "default": true, "examples": true, "$schema": true}
	for k, v := range schema {
		if !allowed[k] {
			return fmt.Errorf("unsupported schema assertion %q", k)
		}
		if k == "type" {
			s, ok := v.(string)
			if !ok || !strings.Contains(" object array string boolean integer number null ", " "+s+" ") {
				return fmt.Errorf("unsupported schema type")
			}
		}
		if k == "properties" {
			p, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("properties must be an object")
			}
			for _, v := range p {
				s, ok := v.(map[string]any)
				if !ok {
					return fmt.Errorf("property schemas must be objects")
				}
				if e := checkSchemaValue(s); e != nil {
					return e
				}
			}
		}
		if k == "items" {
			s, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("items must be an object schema")
			}
			if e := checkSchemaValue(s); e != nil {
				return e
			}
		}
		if k == "pattern" {
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("pattern must be a string")
			}
			if _, e := regexp.Compile(s); e != nil {
				return fmt.Errorf("invalid pattern: %w", e)
			}
		}
		if k == "required" {
			items, ok := v.([]any)
			if !ok {
				return fmt.Errorf("required must be an array")
			}
			for _, item := range items {
				if _, ok := item.(string); !ok {
					return fmt.Errorf("required entries must be strings")
				}
			}
		}
		if k == "additionalProperties" {
			if _, ok := v.(bool); !ok {
				child, ok := v.(map[string]any)
				if !ok {
					return fmt.Errorf("additionalProperties must be boolean or schema")
				}
				if err := checkSchemaValue(child); err != nil {
					return err
				}
			}
		}
		if k == "enum" {
			items, ok := v.([]any)
			if !ok || len(items) == 0 {
				return fmt.Errorf("enum must be a nonempty array")
			}
		}
		if k == "minimum" || k == "maximum" || k == "exclusiveMinimum" || k == "exclusiveMaximum" {
			if _, ok := schemaNumber(v); !ok {
				return fmt.Errorf("%s must be a finite number", k)
			}
		}
		if k == "minLength" || k == "maxLength" || k == "minItems" || k == "maxItems" {
			n, ok := v.(int)
			if !ok || n < 0 {
				return fmt.Errorf("%s must be a nonnegative integer", k)
			}
		}
	}
	return nil
}
