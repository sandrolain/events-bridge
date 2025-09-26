package utils_test

import (
	"strings"
	"testing"

	"github.com/sandrolain/events-bridge/src/utils"
)

func TestLoadPluginMissingFile(t *testing.T) {
	value, err := utils.LoadPlugin[map[string]any, int]("/non/existent/plugin.so", "Constructor", nil)
	if err == nil {
		t.Fatal("expected error when plugin file is missing")
	}
	if !strings.Contains(err.Error(), "failed to open plugin") {
		t.Fatalf("unexpected error: %v", err)
	}
	if value != 0 {
		t.Fatalf("expected zero value for generic type, got %d", value)
	}
}
