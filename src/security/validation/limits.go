package validation

import (
	"fmt"
	"strings"
)

// Message size limits
const (
	MaxMessageDataSize   = 10 << 20 // 10 MB
	MaxMetadataSize      = 1 << 20  // 1 MB
	MaxMetadataKeySize   = 1024     // 1 KB
	MaxMetadataValueSize = 64 << 10 // 64 KB
	MaxMetadataEntries   = 1000
)

// JSON/CBOR limits
const (
	MaxJSONDepth      = 50
	MaxJSONSize       = 10 << 20 // 10 MB
	MaxArrayLength    = 10000
	MaxCBORSize       = 10 << 20 // 10 MB
	MaxCBORDepth      = 50
	MaxCBORArrayElems = 10000
	MaxCBORMapPairs   = 1000
)

// Expression evaluation limits
const (
	MaxExprComplexity = 1000
)

// Config limits
const (
	MaxConfigSize = 1 << 20 // 1 MB
)

// ValidateMessageDataSize checks if message data size is within limits
func ValidateMessageDataSize(size int) error {
	if size > MaxMessageDataSize {
		return fmt.Errorf("message data exceeds maximum size: %d bytes (limit: %d)", size, MaxMessageDataSize)
	}
	return nil
}

// ValidateMetadataSize checks if total metadata size is within limits
func ValidateMetadataSize(metadata map[string]string) error {
	if len(metadata) > MaxMetadataEntries {
		return fmt.Errorf("metadata entries exceed maximum: %d (limit: %d)", len(metadata), MaxMetadataEntries)
	}

	totalSize := 0
	for k, v := range metadata {
		if len(k) > MaxMetadataKeySize {
			return fmt.Errorf("metadata key exceeds maximum size: %d bytes (limit: %d)", len(k), MaxMetadataKeySize)
		}
		if len(v) > MaxMetadataValueSize {
			return fmt.Errorf("metadata value exceeds maximum size: %d bytes (limit: %d)", len(v), MaxMetadataValueSize)
		}
		totalSize += len(k) + len(v)
	}

	if totalSize > MaxMetadataSize {
		return fmt.Errorf("total metadata size exceeds maximum: %d bytes (limit: %d)", totalSize, MaxMetadataSize)
	}

	return nil
}

// ValidateMetadataEntry checks a single metadata key-value pair
func ValidateMetadataEntry(key, value string, currentMetadata map[string]string) error {
	if len(currentMetadata) >= MaxMetadataEntries {
		return fmt.Errorf("metadata entries exceed maximum: %d", MaxMetadataEntries)
	}

	if len(key) > MaxMetadataKeySize {
		return fmt.Errorf("metadata key exceeds maximum size: %d bytes (limit: %d)", len(key), MaxMetadataKeySize)
	}

	if len(value) > MaxMetadataValueSize {
		return fmt.Errorf("metadata value exceeds maximum size: %d bytes (limit: %d)", len(value), MaxMetadataValueSize)
	}

	// Calculate current total size
	currentSize := 0
	for k, v := range currentMetadata {
		currentSize += len(k) + len(v)
	}

	// Add new entry size
	newSize := currentSize + len(key) + len(value)
	if newSize > MaxMetadataSize {
		return fmt.Errorf("total metadata size would exceed maximum: %d bytes (limit: %d)", newSize, MaxMetadataSize)
	}

	return nil
}

// ValidateJSONSize checks if JSON payload size is within limits
func ValidateJSONSize(size int) error {
	if size > MaxJSONSize {
		return fmt.Errorf("JSON payload exceeds maximum size: %d bytes (limit: %d)", size, MaxJSONSize)
	}
	return nil
}

// ValidateCBORSize checks if CBOR payload size is within limits
func ValidateCBORSize(size int) error {
	if size > MaxCBORSize {
		return fmt.Errorf("CBOR payload exceeds maximum size: %d bytes (limit: %d)", size, MaxCBORSize)
	}
	return nil
}

// ValidateJSONStructure validates JSON structure depth and array lengths
func ValidateJSONStructure(v any, depth int) error {
	if depth > MaxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds maximum depth: %d (limit: %d)", depth, MaxJSONDepth)
	}

	switch val := v.(type) {
	case map[string]any:
		for _, item := range val {
			if err := ValidateJSONStructure(item, depth+1); err != nil {
				return err
			}
		}
	case []any:
		if len(val) > MaxArrayLength {
			return fmt.Errorf("JSON array exceeds maximum length: %d (limit: %d)", len(val), MaxArrayLength)
		}
		for _, item := range val {
			if err := ValidateJSONStructure(item, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

// ValidateExpressionComplexity validates expression complexity using a simple heuristic
func ValidateExpressionComplexity(expr string) error {
	complexity := 0

	// Count operators
	operators := []string{"+", "-", "*", "/", "==", "!=", "<", ">", "<=", ">=", "&&", "||", "!", "%"}
	for _, op := range operators {
		complexity += strings.Count(expr, op)
	}

	// Count function calls (approximate by counting opening parentheses)
	complexity += strings.Count(expr, "(")

	// Count array/map access
	complexity += strings.Count(expr, "[")

	if complexity > MaxExprComplexity {
		return fmt.Errorf("expression complexity %d exceeds maximum %d", complexity, MaxExprComplexity)
	}

	return nil
}

// ValidateConfigContentSize checks if config content size is within limits
func ValidateConfigContentSize(size int) error {
	if size > MaxConfigSize {
		return fmt.Errorf("config content exceeds maximum size: %d bytes (limit: %d)", size, MaxConfigSize)
	}
	return nil
}
