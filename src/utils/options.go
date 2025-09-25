package utils

import (
	"fmt"
	"strings"
	"time"
)

// IntValidator defines a function that validates an int value.
type IntValidator func(int) error

// StringValidator defines a function that validates a string value.
type StringValidator func(string) error

// IntMin returns a validator ensuring value >= min.
func IntMin(min int) IntValidator {
	return func(v int) error {
		if v < min {
			return fmt.Errorf("value %d is less than minimum %d", v, min)
		}
		return nil
	}
}

// IntMax returns a validator ensuring value <= max.
func IntMax(max int) IntValidator {
	return func(v int) error {
		if v > max {
			return fmt.Errorf("value %d is greater than maximum %d", v, max)
		}
		return nil
	}
}

func IntGreaterThan(v int) IntValidator {
	return func(i int) error {
		if i <= v {
			return fmt.Errorf("value %d is not greater than %d", i, v)
		}
		return nil
	}
}

// StringNonEmpty validates non-empty strings.
func StringNonEmpty() StringValidator {
	return func(s string) error {
		if s == "" {
			return fmt.Errorf("value cannot be empty")
		}
		return nil
	}
}

// StringOneOf validates that the string equals one of the provided options.
func StringOneOf(options ...string) StringValidator {
	return func(s string) error {
		for _, o := range options {
			if s == o {
				return nil
			}
		}
		return fmt.Errorf("value %q not in allowed set", s)
	}
}

func DurationPositive() func(time.Duration) error {
	return func(d time.Duration) error {
		if d <= 0 {
			return fmt.Errorf("duration %s is not positive", d)
		}
		return nil
	}
}

type OptsParser struct {
	errors []error
}

// OptInt reads an option key from a generic map and coerces it to int.
// Supports int, int64, float64 JSON/YAML decoded numbers. Applies validators in order.
// Returns defaultVal when key is missing or nil.
func (p *OptsParser) OptInt(opts map[string]any, key string, defaultVal int, validators ...IntValidator) int {
	if opts == nil {
		return defaultVal
	}
	raw, ok := opts[key]
	if !ok || raw == nil {
		return defaultVal
	}
	var v int
	switch t := raw.(type) {
	case int:
		v = t
	case int64:
		v = int(t)
	case float64:
		v = int(t)
	default:
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid type %T, expected number", key, raw))
		return 0
	}
	for _, check := range validators {
		if err := check(v); err != nil {
			p.errors = append(p.errors, fmt.Errorf("option %s invalid: %w", key, err))
		}
	}
	return v
}

// OptString reads an option key from a generic map and coerces it to string.
// Applies validators in order. Returns defaultVal when key is missing or nil.
func (p *OptsParser) OptString(opts map[string]any, key string, defaultVal string, validators ...StringValidator) string {
	if opts == nil {
		return defaultVal
	}
	raw, ok := opts[key]
	if !ok || raw == nil {
		return defaultVal
	}
	s, ok := raw.(string)
	if !ok {
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid type %T, expected string", key, raw))
		return ""
	}
	for _, check := range validators {
		if err := check(s); err != nil {
			p.errors = append(p.errors, fmt.Errorf("option %s invalid: %w", key, err))
		}
	}
	return s
}

func (p *OptsParser) OptStringArray(opts map[string]any, key string, defaultVal []string, validators ...StringValidator) []string {
	if opts == nil {
		return defaultVal
	}
	raw, ok := opts[key]
	if !ok || raw == nil {
		return defaultVal
	}
	var arr []string
	switch t := raw.(type) {
	case []string:
		arr = t
	case []any:
		arr = make([]string, 0, len(t))
		for i, it := range t {
			s, ok := it.(string)
			if !ok {
				p.errors = append(p.errors, fmt.Errorf("option %s[%d] has invalid type %T, expected string", key, i, it))
				continue
			}
			arr = append(arr, s)
		}
	default:
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid type %T, expected array", key, raw))
		return nil
	}
	for i, s := range arr {
		for _, check := range validators {
			if err := check(s); err != nil {
				p.errors = append(p.errors, fmt.Errorf("option %s[%d] invalid: %w", key, i, err))
			}
		}
	}
	return arr
}

func (p *OptsParser) OptStringMap(opts map[string]any, key string, defaultVal map[string]string) map[string]string {
	if opts == nil {
		return defaultVal
	}
	raw, ok := opts[key]
	if !ok || raw == nil {
		return defaultVal
	}
	var result map[string]string
	switch t := raw.(type) {
	case string:
		pairs := strings.Split(t, ";")
		result = make(map[string]string, len(pairs))
		for _, p := range pairs {
			kv := strings.SplitN(p, ":", 2)
			if len(kv) == 2 {
				k := strings.TrimSpace(kv[0])
				v := strings.TrimSpace(kv[1])
				if k != "" {
					result[k] = v
				}
			}
		}
	case map[string]string:
		result = t
	case map[string]any:
		result = make(map[string]string, len(t))
		for k, v := range t {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
	default:
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid type %T, expected map[string]string", key, raw))
		return nil
	}
	return result
}

func (p *OptsParser) OptBool(opts map[string]any, key string, defaultVal bool, validators ...func(bool) error) bool {
	if opts == nil {
		return defaultVal
	}
	raw, ok := opts[key]
	if !ok || raw == nil {
		return defaultVal
	}
	vs, ok := raw.(string)
	if ok {
		vs = strings.ToLower(strings.TrimSpace(vs))
		if vs == "true" || vs == "1" || vs == "yes" {
			return true
		}
		if vs == "false" || vs == "0" || vs == "no" {
			return false
		}
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid bool string %q", key, raw))
		return false
	}
	v, ok := raw.(bool)
	if !ok {
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid type %T, expected bool", key, raw))
		return false
	}
	for _, check := range validators {
		if err := check(v); err != nil {
			p.errors = append(p.errors, fmt.Errorf("option %s invalid: %w", key, err))
		}
	}
	return v
}

func (p *OptsParser) OptDuration(opts map[string]any, key string, defaultVal time.Duration, validators ...func(time.Duration) error) time.Duration {
	if opts == nil {
		return defaultVal
	}
	raw, ok := opts[key]
	if !ok || raw == nil {
		return defaultVal
	}
	var v time.Duration
	switch t := raw.(type) {
	case string:
		d, err := time.ParseDuration(t)
		if err != nil {
			p.errors = append(p.errors, fmt.Errorf("option %s has invalid duration string %q: %w", key, t, err))
			return 0
		}
		v = d
	case int:
		v = time.Duration(t)
	case int64:
		v = time.Duration(t)
	case float64:
		v = time.Duration(int64(t))
	default:
		p.errors = append(p.errors, fmt.Errorf("option %s has invalid type %T, expected number", key, raw))
		return 0
	}
	for _, check := range validators {
		if err := check(v); err != nil {
			p.errors = append(p.errors, fmt.Errorf("option %s invalid: %w", key, err))
		}
	}
	return v
}

func (p *OptsParser) Error() error {
	if len(p.errors) > 0 {
		return &OptsError{Errors: p.errors}
	}
	return nil
}

type OptsError struct {
	Errors []error
}

func (e *OptsError) Error() string {
	s := "options validation errors:"
	for _, err := range e.Errors {
		s += "\n - " + err.Error()
	}
	return s
}
