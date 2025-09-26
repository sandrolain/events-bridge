package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/common"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
)

var _ connectors.Source = &PluginSource{}

type SourceConfig struct {
	Plugin  plugin.PluginConfig `mapstructure:"plugin" validate:"required,dive"`
	Config  map[string]string   `mapstructure:"config"`
	Timeout time.Duration       `mapstructure:"timeout" default:"5s" validate:"required,gt=0"`
}

type PluginSource struct {
	cfg   *SourceConfig
	slog  *slog.Logger
	c     <-chan *message.RunnerMessage
	plg   *plugin.Plugin
	close func()
}

func NewSource(opts map[string]any) (connectors.Source, error) {
	cfg, err := common.ParseConfig[SourceConfig](opts)
	if err != nil {
		return nil, err
	}

	mgr, err := plugin.GetPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin manager: %w", err)
	}

	plg, err := mgr.GetOrCreatePlugin(cfg.Plugin)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Plugin.Name, err)
	}

	ps := &PluginSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "Plugin Source", "id", cfg.Plugin.Name),
		plg:  plg,
	}
	return ps, nil
}

func (s *PluginSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.slog.Info("starting plugin source", "id", s.cfg.Plugin.Name, "exec", s.cfg.Plugin.Exec)

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()

	var err error
	s.c, s.close, err = s.plg.Source(ctx, buffer, s.cfg.Config)
	if err != nil {
		return nil, err
	}
	return s.c, nil
}

func (s *PluginSource) Close() error {
	if s.close != nil {
		s.close()
	}
	return nil
}
