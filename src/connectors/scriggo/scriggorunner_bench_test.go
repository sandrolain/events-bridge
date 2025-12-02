package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

const (
	benchID             = "bench-id"
	benchData           = "benchmark test data"
	benchDataComplex    = "benchmark test data for complex processing"
	benchSource         = "bench"
	errMsgBenchWrite    = "failed to write script file: %v"
	errMsgBenchCreate   = "failed to create runner: %v"
	errMsgBenchProcess  = "process returned error: %v"
	benchScriptFileName = "script.go"
)

func BenchmarkScriggoRunnerProcess(b *testing.B) {
	dir := b.TempDir()
	scriptPath := filepath.Join(dir, benchScriptFileName)
	script := `package main

import "events"

func main() {
	data, _ := events.Message.GetData()
	events.Message.SetData(data)
	events.Message.AddMetadata("processed", "true")
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		b.Fatalf(errMsgBenchWrite, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf(errMsgBenchCreate, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		b.Fatal("failed to cast runner to ScriggoRunner")
	}

	stub := testutil.NewAdapter([]byte(benchData), map[string]string{"source": benchSource})
	stub.ID = []byte(benchID)
	msg := message.NewRunnerMessage(stub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.Process(msg); err != nil {
			b.Fatalf(errMsgBenchProcess, err)
		}
	}
}

func BenchmarkScriggoRunnerProcessComplex(b *testing.B) {
	dir := b.TempDir()
	scriptPath := filepath.Join(dir, benchScriptFileName)
	script := `package main

import "events"

func main() {
	data, _ := events.Message.GetData()
	
	// Simulate some processing
	result := make([]byte, len(data))
	for i, b := range data {
		result[i] = byte((int(b) + i) % 256)
	}
	
	events.Message.SetData(result)
	
	// Add multiple metadata entries
	for i := 0; i < 5; i++ {
		key := "key_" + string(rune('0'+i))
		value := "value_" + string(rune('0'+i))
		events.Message.AddMetadata(key, value)
	}
	
	events.Message.AddMetadata("processed", "true")
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		b.Fatalf(errMsgBenchWrite, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf(errMsgBenchCreate, err)
	}
	runner, ok := runnerAny.(*ScriggoRunner)
	if !ok {
		b.Fatal("failed to cast to ScriggoRunner")
	}

	stub := testutil.NewAdapter([]byte(benchDataComplex), map[string]string{"source": benchSource, "type": "test"})
	stub.ID = []byte(benchID)
	msg := message.NewRunnerMessage(stub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.Process(msg); err != nil {
			b.Fatalf(errMsgBenchProcess, err)
		}
	}
}

func BenchmarkScriggoRunnerCreation(b *testing.B) {
	dir := b.TempDir()
	scriptPath := filepath.Join(dir, benchScriptFileName)
	script := `package main

import "events"

func main() {
	events.Message.SetData([]byte("HELLO"))
	events.Message.AddMetadata("processed", "true")
}
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		b.Fatalf(errMsgBenchWrite, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runner, err := NewRunner(cfg)
		if err != nil {
			b.Fatalf(errMsgBenchCreate, err)
		}
		if err := runner.Close(); err != nil {
			b.Logf("failed to close runner: %v", err)
		}
	}
}
