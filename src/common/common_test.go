package common

import "testing"

func TestCopyMapNoDest(t *testing.T) {
	src := map[string]string{"key1": "value1", "key2": "value2"}
	dst := CopyMap(src, nil)

	if &src == &dst {
		t.Errorf("Expected a new map, but got the same reference")
	}

	if len(dst) != len(src) {
		t.Errorf("Expected copied map length %d, got %d", len(src), len(dst))
	}

	for k, v := range src {
		if dst[k] != v {
			t.Errorf("Expected value for key '%s' to be '%s', got '%s'", k, v, dst[k])
		}
	}
}

func TestCopyMapWithDest(t *testing.T) {
	src := map[string]string{"key1": "value1", "key2": "value2"}
	dst := map[string]string{"existingKey": "existingValue", "key1": "oldValue"}

	result := CopyMap(src, dst)

	expectedLen := len(src) + 1 // +1 for the existing key in dst
	if len(result) != expectedLen {
		t.Errorf("Expected copied map length %d, got %d", expectedLen, len(result))
	}

	for k, v := range src {
		if result[k] != v {
			t.Errorf("Expected value for key '%s' to be '%s', got '%s'", k, v, result[k])
		}
	}

	if result["existingKey"] != "existingValue" {
		t.Errorf("Expected existing key to remain unchanged with value 'existingValue', got '%s'", result["existingKey"])
	}
}
