package main

import (
	"encoding/json"
	"fmt"
)

const (
	// Maximum JSON size to prevent DoS
	defaultMaxLogicSize = 100000 // 100KB
	// Maximum nesting depth
	defaultMaxNestingDepth = 50
)

// validateLogicJSON performs security checks on JSONLogic rules
func validateLogicJSON(logicBytes []byte, maxSize int) error {
	if maxSize == 0 {
		maxSize = defaultMaxLogicSize
	}

	// Check size
	if len(logicBytes) > maxSize {
		return fmt.Errorf("logic JSON exceeds maximum size of %d bytes", maxSize)
	}

	// Check for empty
	if len(logicBytes) == 0 {
		return fmt.Errorf("logic JSON cannot be empty")
	}

	// Validate JSON structure
	var logic map[string]interface{}
	if err := json.Unmarshal(logicBytes, &logic); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Check nesting depth
	depth := calculateNestingDepth(logic)
	if depth > defaultMaxNestingDepth {
		return fmt.Errorf("logic JSON has excessive nesting depth: %d (max: %d)", depth, defaultMaxNestingDepth)
	}

	return nil
}

// calculateNestingDepth recursively calculates the maximum nesting depth
func calculateNestingDepth(data interface{}) int {
	switch v := data.(type) {
	case map[string]interface{}:
		maxDepth := 0
		for _, value := range v {
			depth := calculateNestingDepth(value)
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth + 1
	case []interface{}:
		maxDepth := 0
		for _, value := range v {
			depth := calculateNestingDepth(value)
			if depth > maxDepth {
				maxDepth = depth
			}
		}
		return maxDepth + 1
	default:
		return 0
	}
}

// validateAllowedOperations checks if the logic only uses allowed operations
func validateAllowedOperations(logic map[string]interface{}, allowedOps []string) error {
	if len(allowedOps) == 0 {
		return nil // No restrictions
	}

	// Extract all operations from the logic
	operations := extractOperations(logic)

	for op := range operations {
		allowed := false
		for _, allowedOp := range allowedOps {
			if op == allowedOp {
				allowed = true
				break
			}
		}

		if !allowed {
			return fmt.Errorf("operation '%s' is not in the allowlist", op)
		}
	}

	return nil
}

// extractOperations recursively extracts all operation names from JSONLogic
func extractOperations(data interface{}) map[string]bool {
	operations := make(map[string]bool)

	switch v := data.(type) {
	case map[string]interface{}:
		for key, value := range v {
			// The key is typically the operation name in JSONLogic
			operations[key] = true
			// Recursively check nested structures
			for nestedOp := range extractOperations(value) {
				operations[nestedOp] = true
			}
		}
	case []interface{}:
		for _, item := range v {
			for nestedOp := range extractOperations(item) {
				operations[nestedOp] = true
			}
		}
	}

	return operations
}

// countOperations counts the total number of operations in the logic
func countOperations(data interface{}) int {
	count := 0

	switch v := data.(type) {
	case map[string]interface{}:
		count += len(v)
		for _, value := range v {
			count += countOperations(value)
		}
	case []interface{}:
		for _, item := range v {
			count += countOperations(item)
		}
	}

	return count
}

// validateComplexity checks if the logic is not too complex
func validateComplexity(logic map[string]interface{}, maxOps int) error {
	if maxOps == 0 {
		maxOps = 1000 // default
	}

	count := countOperations(logic)
	if count > maxOps {
		return fmt.Errorf("logic is too complex: %d operations (max: %d)", count, maxOps)
	}

	return nil
}
