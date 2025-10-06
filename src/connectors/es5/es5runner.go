package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/dop251/goja"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

// Ensure ES5Runner implements models.Runner
var _ connectors.Runner = &ES5Runner{}

type RunnerConfig struct {
	Path     string                        `mapstructure:"path" validate:"required"`
	Timeout  time.Duration                 `mapstructure:"timeout" default:"5s" validate:"gt=0"`
	Services map[string]connectors.Service `mapstructure:"services,omitempty"`
}

type ES5Runner struct {
	cfg     *RunnerConfig
	slog    *slog.Logger
	program *goja.Program
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// New creates a new instance of ES5Runner
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	log := slog.Default().With("context", "ES5 Runner")
	log.Info("loading es5 program", "path", cfg.Path)

	src, err := os.ReadFile(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to read js file: %w", err)
	}

	name := filepath.Base(cfg.Path)

	prog, err := goja.Compile(name, string(src), true)
	if err != nil {
		return nil, fmt.Errorf("failed to compile js: %w", err)
	}

	return &ES5Runner{
		cfg:     cfg,
		slog:    log,
		program: prog,
	}, nil
}

// processMessage handles the logic for a single message
func (e *ES5Runner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.Timeout)
	defer cancel()

	vm := goja.New()
	//vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	for name, service := range e.cfg.Services {
		dyn := &DynamicObject{runtime: vm, service: service}
		obj := vm.NewDynamicObject(dyn)
		vm.Set(name, obj)
	}

	result := msg
	if err := vm.Set("message", result); err != nil {
		return fmt.Errorf("failed to set message: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("js panic: %v", r)
			}
		}()
		_, err := vm.RunProgram(e.program)
		done <- err
	}()

	select {
	case <-ctx.Done():
		e.slog.Warn("js execution timeout, potential goroutine leak")
		return fmt.Errorf("js execution timeout")
	case err := <-done:
		if err != nil {
			return fmt.Errorf("js execution error: %w", err)
		}
	}

	return nil
}

func (e *ES5Runner) Close() error {
	return nil
}

type DynamicObject struct {
	runtime *goja.Runtime
	service connectors.Service
}

func (d *DynamicObject) Get(name string) goja.Value {
	// Restituisce una funzione JS che richiama il callback Go
	return d.runtime.ToValue(func(call goja.FunctionCall) goja.Value {
		fmt.Printf("Called dynamic method: %s, args: %v\n", name, call.Arguments)
		return d.runtime.ToValue("ok-" + name)
	})
}
