package main

import (
	"strings"
	"testing"
)

func TestValidateLogicJSON(t *testing.T) {
	tests := []struct {
		name        string
		logic       string
		maxSize     int
		shouldError bool
	}{
		{
			name:        "valid simple logic",
			logic:       `{"==": [{"var": "data.value"}, 10]}`,
			maxSize:     1000,
			shouldError: false,
		},
		{
			name:        "valid complex logic",
			logic:       `{"and": [{">":[{"var":"data.temp"},20]},{"<":[{"var":"data.humidity"},80]}]}`,
			maxSize:     1000,
			shouldError: false,
		},
		{
			name:        "empty logic",
			logic:       "",
			maxSize:     1000,
			shouldError: true,
		},
		{
			name:        "logic too large",
			logic:       `{"var": "` + strings.Repeat("a", 100001) + `"}`,
			maxSize:     100000,
			shouldError: true,
		},
		{
			name:        "invalid JSON",
			logic:       `{invalid json}`,
			maxSize:     1000,
			shouldError: true,
		},
		{
			name:        "excessive nesting",
			logic:       createDeeplyNestedJSON(60),
			maxSize:     100000,
			shouldError: true,
		},
		{
			name:        "moderate nesting",
			logic:       createDeeplyNestedJSON(20),
			maxSize:     10000,
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLogicJSON([]byte(tt.logic), tt.maxSize)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

func TestCalculateNestingDepth(t *testing.T) {
	tests := []struct {
		name          string
		data          interface{}
		expectedDepth int
	}{
		{
			name:          "simple value",
			data:          "string",
			expectedDepth: 0,
		},
		{
			name: "single level map",
			data: map[string]interface{}{
				"key": "value",
			},
			expectedDepth: 1,
		},
		{
			name: "nested map",
			data: map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": "value",
					},
				},
			},
			expectedDepth: 3,
		},
		{
			name: "array with nested map",
			data: []interface{}{
				map[string]interface{}{
					"nested": "value",
				},
			},
			expectedDepth: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			depth := calculateNestingDepth(tt.data)
			if depth != tt.expectedDepth {
				t.Errorf("expected depth %d, got %d", tt.expectedDepth, depth)
			}
		})
	}
}

func TestValidateAllowedOperations(t *testing.T) {
	tests := []struct {
		name           string
		logic          map[string]interface{}
		allowedOps     []string
		shouldError    bool
		expectedErrMsg string
	}{
		{
			name: "no restrictions",
			logic: map[string]interface{}{
				"==": []interface{}{1, 2},
			},
			allowedOps:  nil,
			shouldError: false,
		},
		{
			name: "allowed operation",
			logic: map[string]interface{}{
				"==": []interface{}{1, 2},
			},
			allowedOps:  []string{"==", "!=", ">", "<"},
			shouldError: false,
		},
		{
			name: "disallowed operation",
			logic: map[string]interface{}{
				"custom": []interface{}{1, 2},
			},
			allowedOps:     []string{"==", "!=", ">", "<"},
			shouldError:    true,
			expectedErrMsg: "custom",
		},
		{
			name: "multiple operations all allowed",
			logic: map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{"==": []interface{}{1, 2}},
					map[string]interface{}{">": []interface{}{3, 4}},
				},
			},
			allowedOps:  []string{"and", "==", ">"},
			shouldError: false,
		},
		{
			name: "one disallowed among allowed",
			logic: map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{"==": []interface{}{1, 2}},
					map[string]interface{}{"custom": []interface{}{3, 4}},
				},
			},
			allowedOps:     []string{"and", "==", ">"},
			shouldError:    true,
			expectedErrMsg: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAllowedOperations(tt.logic, tt.allowedOps)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
			if tt.shouldError && err != nil && tt.expectedErrMsg != "" {
				if !strings.Contains(err.Error(), tt.expectedErrMsg) {
					t.Errorf("expected error to contain '%s', got: %v", tt.expectedErrMsg, err)
				}
			}
		})
	}
}

func TestCountOperations(t *testing.T) {
	tests := []struct {
		name          string
		logic         interface{}
		expectedCount int
	}{
		{
			name:          "simple value",
			logic:         "value",
			expectedCount: 0,
		},
		{
			name: "single operation",
			logic: map[string]interface{}{
				"==": []interface{}{1, 2},
			},
			expectedCount: 1,
		},
		{
			name: "nested operations",
			logic: map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{"==": []interface{}{1, 2}},
					map[string]interface{}{">": []interface{}{3, 4}},
				},
			},
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := countOperations(tt.logic)
			if count != tt.expectedCount {
				t.Errorf("expected count %d, got %d", tt.expectedCount, count)
			}
		})
	}
}

func TestValidateComplexity(t *testing.T) {
	tests := []struct {
		name        string
		logic       map[string]interface{}
		maxOps      int
		shouldError bool
	}{
		{
			name: "simple logic within limit",
			logic: map[string]interface{}{
				"==": []interface{}{1, 2},
			},
			maxOps:      10,
			shouldError: false,
		},
		{
			name:        "complex logic exceeds limit",
			logic:       createComplexLogic(1500),
			maxOps:      1000,
			shouldError: true,
		},
		{
			name:        "complex logic within limit",
			logic:       createComplexLogic(500),
			maxOps:      1000,
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateComplexity(tt.logic, tt.maxOps)
			if tt.shouldError && err == nil {
				t.Errorf("expected error but got none")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}
		})
	}
}

// Helper functions

func createDeeplyNestedJSON(depth int) string {
	if depth == 0 {
		return `{"value": 1}`
	}
	inner := createDeeplyNestedJSON(depth - 1)
	return `{"nested": ` + inner + `}`
}

func createComplexLogic(numOps int) map[string]interface{} {
	result := make(map[string]interface{})
	current := result

	for i := 0; i < numOps; i++ {
		nested := make(map[string]interface{})
		current["op"+string(rune(i))] = nested
		current = nested
	}

	return result
}
