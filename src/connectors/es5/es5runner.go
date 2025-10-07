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
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))

	for name, service := range e.cfg.Services {
		vm.Set(name, &ServiceProxy{svc: service})
	}

	vm.Set("console", &consoleProxy{slog: e.slog.With("script", e.cfg.Path)})
	vm.Set("util", &utilProxy{})

	if err := vm.Set("message", &messageProxy{msg: msg}); err != nil {
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

type ServiceProxy struct {
	svc connectors.Service
}

func (s *ServiceProxy) Call(name string, args ...any) any {
	if !s.svc.IsValidMethod(name, args) {
		panic(fmt.Sprintf("invalid method: %s", name))
	}
	res, err := s.svc.Call(name, args)
	if err != nil {
		panic(err)
	}
	return res
}

type messageProxy struct {
	msg *message.RunnerMessage
}

func (m *messageProxy) GetData() []byte {
	d, e := m.msg.GetData()
	if e != nil {
		panic(e)
	}
	return d
}

func (m *messageProxy) GetDataString() string {
	d := m.GetData()
	return string(d)
}

func (m *messageProxy) SetData(d []byte) {
	m.msg.SetData(d)
}

func (m *messageProxy) SetDataString(d string) {
	m.msg.SetData([]byte(d))
}

func (m *messageProxy) SetMetadata(key, value string) {
	m.msg.AddMetadata(key, value)
}

func (m *messageProxy) GetMetadata(key string) string {
	d, e := m.msg.GetMetadata()
	if e != nil {
		panic(e)
	}
	v, ok := d[key]
	if !ok {
		return ""
	}
	return v
}

type consoleProxy struct {
	slog *slog.Logger
}

func (c *consoleProxy) Log(msg ...any) {
	c.slog.Info(fmt.Sprint(msg...))
}

func (c *consoleProxy) Info(msg ...any) {
	c.slog.Info(fmt.Sprint(msg...))
}

func (c *consoleProxy) Warn(msg ...any) {
	c.slog.Warn(fmt.Sprint(msg...))
}

func (c *consoleProxy) Error(msg ...any) {
	c.slog.Error(fmt.Sprint(msg...))
}

type utilProxy struct{}

func (u *utilProxy) BytesToString(b []byte) string {
	return string(b)
}

func (u *utilProxy) StringToBytes(s string) []byte {
	return []byte(s)
}
