package expreval

import (
	"strings"
	"testing"
)

func TestValidateExpression(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		maxLength   int
		shouldError bool
	}{
		{
			name:        "valid simple expression",
			expression:  "data.value > 10",
			maxLength:   100,
			shouldError: false,
		},
		{
			name:        "valid complex expression",
			expression:  "data.temperature > 20 && data.humidity < 80",
			maxLength:   100,
			shouldError: false,
		},
		{
			name:        "empty expression",
			expression:  "",
			maxLength:   100,
			shouldError: true,
		},
		{
			name:        "whitespace only expression",
			expression:  "   ",
			maxLength:   100,
			shouldError: true,
		},
		{
			name:        "expression too long",
			expression:  strings.Repeat("a", 10001),
			maxLength:   10000,
			shouldError: true,
		},
		{
			name:        "expression with default max length",
			expression:  "data.value > 10",
			maxLength:   0, // Will use default
			shouldError: false,
		},
		{
			name:        "very complex expression",
			expression:  strings.Repeat("(a && b) || ", 1000) + "c",
			maxLength:   20000,
			shouldError: true, // Should fail on complexity
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExpression(tt.expression, tt.maxLength)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestEstimateComplexity(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		minScore   int // We check if score is at least this value
	}{
		{
			name:       "simple expression",
			expression: "a + b",
			minScore:   1,
		},
		{
			name:       "multiple operators",
			expression: "a + b - c * d / e",
			minScore:   4,
		},
		{
			name:       "with function calls",
			expression: "max(a, b) + min(c, d)",
			minScore:   4, // 2 functions + 1 operator
		},
		{
			name:       "complex expression",
			expression: "data.temperature > 20 && data.humidity < 80 || data.pressure >= 1000",
			minScore:   4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := estimateComplexity(tt.expression)
			if score < tt.minScore {
				t.Errorf("expected complexity score >= %d, got %d", tt.minScore, score)
			}
		})
	}
}

func TestValidateAllowedFunctions(t *testing.T) {
	tests := []struct {
		name             string
		expression       string
		allowedFunctions []string
		shouldError      bool
	}{
		{
			name:             "no restrictions",
			expression:       "max(a, b) + min(c, d)",
			allowedFunctions: nil,
			shouldError:      false,
		},
		{
			name:             "allowed function",
			expression:       "max(a, b)",
			allowedFunctions: []string{"max", "min"},
			shouldError:      false,
		},
		{
			name:             "disallowed function",
			expression:       "custom(a, b)",
			allowedFunctions: []string{"max", "min"},
			shouldError:      true,
		},
		{
			name:             "multiple allowed functions",
			expression:       "max(a, b) + min(c, d)",
			allowedFunctions: []string{"max", "min"},
			shouldError:      false,
		},
		{
			name:             "one disallowed among allowed",
			expression:       "max(a, b) + custom(c, d)",
			allowedFunctions: []string{"max", "min"},
			shouldError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAllowedFunctions(tt.expression, tt.allowedFunctions)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestDetectDangerousPatterns(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		shouldError bool
	}{
		{
			name:        "safe expression",
			expression:  "data.value > 10",
			shouldError: false,
		},
		{
			name:        "infinite loop while",
			expression:  "while(true) { doSomething() }",
			shouldError: true,
		},
		{
			name:        "infinite loop for",
			expression:  "for(;;) { doSomething() }",
			shouldError: true,
		},
		{
			name:        "excessive nesting",
			expression:  strings.Repeat("(", 60) + "a" + strings.Repeat(")", 60),
			shouldError: true,
		},
		{
			name:        "moderate nesting",
			expression:  strings.Repeat("(", 20) + "a" + strings.Repeat(")", 20),
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := detectDangerousPatterns(tt.expression)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestNewExprEvaluatorWithConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		shouldError bool
	}{
		{
			name: "valid configuration",
			config: Config{
				Expression:          "data.value > 10",
				MaxExpressionLength: 100,
				AllowedFunctions:    nil,
				AllowUndefined:      true,
			},
			shouldError: false,
		},
		{
			name: "invalid expression - too long",
			config: Config{
				Expression:          strings.Repeat("a", 10001),
				MaxExpressionLength: 10000,
			},
			shouldError: true,
		},
		{
			name: "invalid expression - disallowed function",
			config: Config{
				Expression:       "custom(a, b)",
				AllowedFunctions: []string{"max", "min"},
			},
			shouldError: true,
		},
		{
			name: "valid with allowed functions",
			config: Config{
				Expression:       "max(a, b)",
				AllowedFunctions: []string{"max", "min"},
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewExprEvaluatorWithConfig(tt.config)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}
