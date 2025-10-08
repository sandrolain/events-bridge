package main

import (
	"testing"
	"time"
)

func BenchmarkWasmRunnerProcess(b *testing.B) {
	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
		Timeout: 5 * time.Second,
		Format:  "cli",
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf(errMsgCreateRunner, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			b.Errorf(msgErrClosingRunner, err)
		}
	}()

	wasmRunner := runner.(*WasmRunner)
	msg := createTestMessage()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wasmRunner.Process(msg); err != nil {
			b.Fatalf(errMsgProcessReturned, err)
		}
	}
}

func BenchmarkWasmRunnerProcessParallel(b *testing.B) {
	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
		Timeout: 5 * time.Second,
		Format:  "cli",
	}

	runner, err := NewRunner(cfg)
	if err != nil {
		b.Fatalf(errMsgCreateRunner, err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			b.Errorf(msgErrClosingRunner, err)
		}
	}()

	wasmRunner := runner.(*WasmRunner)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		msg := createTestMessage()
		for pb.Next() {
			if err := wasmRunner.Process(msg); err != nil {
				b.Errorf(errMsgProcessReturned, err)
			}
		}
	})
}

func BenchmarkWasmRunnerCreation(b *testing.B) {
	cfg := &RunnerConfig{
		Path:    getTestAssetPath(testWasmFile),
		Timeout: 5 * time.Second,
		Format:  "cli",
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
