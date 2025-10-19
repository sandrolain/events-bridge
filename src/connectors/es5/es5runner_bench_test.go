package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/testutil"
)

func BenchmarkES5RunnerProcess(b *testing.B) {
	dir := b.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `
// Simple data transformation
var data = message.GetData();
var upperData = new Uint8Array(data.length);
for (var i = 0; i < data.length; i++) {
	upperData[i] = data[i];
}
message.SetData(upperData);
message.AddMetadata("processed", "true");
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		b.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf(errMsgCreateRunner, err)
	}
	runner := runnerAny.(*ES5Runner)

	stub := testutil.NewAdapter([]byte(benchData), map[string]string{"source": benchSource})
	stub.ID = []byte(benchID)
	msg := message.NewRunnerMessage(stub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.Process(msg); err != nil {
			b.Fatalf(errMsgProcessReturned, err)
		}
	}
}

func BenchmarkES5RunnerProcessComplex(b *testing.B) {
	dir := b.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `
// More complex processing with loops and transformations
var data = message.GetData();
var metadata = message.GetMetadata();
var result = new Uint8Array(data.length);

// Simulate some processing
for (var i = 0; i < data.length; i++) {
	result[i] = (data[i] + i) % 256;
}

message.SetData(result);

// Add multiple metadata entries
for (var i = 0; i < 5; i++) {
	message.AddMetadata("key_" + i, "value_" + i);
}

message.AddMetadata("processed", "true");
message.AddMetadata("timestamp", new Date().toISOString());
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		b.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	runnerAny, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf(errMsgCreateRunner, err)
	}
	runner := runnerAny.(*ES5Runner)

	stub := testutil.NewAdapter([]byte(benchDataComplex), map[string]string{"source": benchSource, "type": "test"})
	stub.ID = []byte(benchID)
	msg := message.NewRunnerMessage(stub)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.Process(msg); err != nil {
			b.Fatalf(errMsgProcessReturned, err)
		}
	}
}

func BenchmarkES5RunnerCreation(b *testing.B) {
	dir := b.TempDir()
	scriptPath := filepath.Join(dir, scriptFileName)
	script := `
message.SetData(new Uint8Array([72, 69, 76, 76, 79]));
message.AddMetadata("processed", "true");
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		b.Fatalf(errMsgWriteScript, err)
	}

	cfg := &RunnerConfig{
		Path:    scriptPath,
		Timeout: 5 * time.Second,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runner, err := NewRunner(cfg)
		if err != nil {
			b.Fatalf(errMsgCreateRunner, err)
		}
		_ = runner.Close()
	}
}
