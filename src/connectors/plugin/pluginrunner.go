package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
)

var _ connectors.Runner = &PluginRunner{}

type RunnerConfig struct {
	Plugin  plugin.PluginConfig `mapstructure:"plugin" validate:"required"`
	Timeout time.Duration       `mapstructure:"timeout" default:"5s" validate:"required"`
}

type PluginRunner struct {
	cfg  *RunnerConfig
	slog *slog.Logger
	plg  *plugin.Plugin
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// New creates a new instance of PluginRunner
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	mgr, err := plugin.GetPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin manager: %w", err)
	}

	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}

	plg, err := mgr.GetOrCreatePlugin(cfg.Plugin)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Plugin.Name, err)
	}
	return &PluginRunner{
		cfg:  cfg,
		slog: slog.Default().With("context", "Plugin Runner", "id", cfg.Plugin.Name),
		plg:  plg,
	}, nil
}

func (p *PluginRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Timeout)
	defer cancel()
	return p.plg.Runner(ctx, msg)
}

func (p *PluginRunner) Close() error {
	return nil
}
