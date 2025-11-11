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

	// Test NewRunnerConfig
	runnerCfg := NewRunnerConfig()
	_, ok = runnerCfg.(*RunnerConfig)
	if !ok {
		t.Fatal("NewRunnerConfig should return *RunnerConfig")
	}
}
