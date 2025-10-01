package main

import (
	"testing"
	"time"
)

// Test for CLI Runner functions (0% coverage)
func TestCLIRunner(t *testing.T) {
	// Test NewRunnerConfig
	cfg := NewRunnerConfig()
	runnerCfg, ok := cfg.(*RunnerConfig)
	if !ok {
		t.Fatal("NewRunnerConfig should return *RunnerConfig")
	}
	if runnerCfg == nil {
		t.Fatal("NewRunnerConfig should not return nil")
	}

	// Test NewRunner with valid config
	runnerCfg.Command = "echo"
	runnerCfg.Args = []string{"test"}
	runnerCfg.Timeout = 5 * time.Second

	runner, err := NewRunner(runnerCfg)
	if err != nil {
		t.Fatalf("NewRunner error: %v", err)
	}

	// Test Close (skip Process test as it requires specific input format)
	err = runner.Close()
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}
}
