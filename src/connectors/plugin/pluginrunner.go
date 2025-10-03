package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/manager"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ connectors.Runner = &PluginRunner{}

type RunnerConfig struct {
	Plugin  manager.PluginConfig `mapstructure:"plugin" validate:"required"`
	Timeout time.Duration        `mapstructure:"timeout" default:"5s" validate:"required"`
}

type PluginRunner struct {
	cfg  *RunnerConfig
	slog *slog.Logger
	plg  *manager.Plugin
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

	mgr, err := manager.GetPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin manager: %w", err)
	}

	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}

	plg, err := mgr.GetOrCreatePlugin(cfg.Plugin, true)
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
	uid := msg.GetID()

	metadata, e := msg.GetMetadata()
	if e != nil {
		return nil, fmt.Errorf("failed to get message metadata: %w", e)
	}

	data, e := msg.GetData()
	if e != nil {
		return nil, fmt.Errorf("failed to get message data: %w", e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.Timeout)
	defer cancel()
	res, err := p.plg.Runner(ctx, uid, metadata, data)
	if err != nil {
		return nil, fmt.Errorf("failed to process message: %w", err)
	}
	return message.NewRunnerMessage(res), nil
}

func (p *PluginRunner) Close() error {
	return nil
}
