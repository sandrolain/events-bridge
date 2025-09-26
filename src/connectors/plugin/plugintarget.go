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

var _ connectors.Target = &PluginTarget{}

type TargetConfig struct {
	Plugin  plugin.PluginConfig `mapstructure:"plugin" validate:"required"`
	Config  map[string]string   `mapstructure:"config"`
	Timeout time.Duration       `mapstructure:"timeout" default:"5s" validate:"required,gt=0"`
}

type PluginTarget struct {
	cfg    *TargetConfig
	slog   *slog.Logger
	plg    *plugin.Plugin
	stopCh chan struct{}
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	mgr, err := plugin.GetPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin manager: %w", err)
	}

	plg, err := mgr.GetOrCreatePlugin(cfg.Plugin, true)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Plugin.Name, err)
	}

	t := &PluginTarget{
		cfg:    cfg,
		slog:   slog.Default().With("context", "Plugin Target", "id", cfg.Plugin.Name),
		plg:    plg,
		stopCh: make(chan struct{}),
	}
	return t, nil
}

func (t *PluginTarget) Consume(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.Timeout)
	defer cancel()
	return t.plg.Target(ctx, msg)
}

func (t *PluginTarget) Close() error {
	if t.stopCh != nil {
		close(t.stopCh)
	}
	if t.plg != nil {
		t.plg.Stop()
	}
	return nil
}
