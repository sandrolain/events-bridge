package main

import (
	"testing"
)

// Test for config constructors (0% coverage)
func TestConfigConstructors(t *testing.T) {
	// Test NewSourceConfig
	srcCfg := NewSourceConfig()
	_, ok := srcCfg.(*SourceConfig)
	if !ok {
		t.Fatal("NewSourceConfig should return *SourceConfig")
	}

	// Test NewTargetConfig
	tgtCfg := NewTargetConfig()
	_, ok = tgtCfg.(*TargetConfig)
	if !ok {
		t.Fatal("NewTargetConfig should return *TargetConfig")
	}
}
