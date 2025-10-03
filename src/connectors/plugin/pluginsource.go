package main

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/connectors/plugin/manager"
	"github.com/sandrolain/events-bridge/src/message"
)

var _ connectors.Source = &PluginSource{}

type SourceConfig struct {
	Plugin manager.PluginConfig `mapstructure:"plugin" validate:"required"`
	Config map[string]string    `mapstructure:"config"`
}

type PluginSource struct {
	cfg   *SourceConfig
	slog  *slog.Logger
	plg   *manager.Plugin
	close func()
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	mgr, err := manager.GetPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin manager: %w", err)
	}

	plg, err := mgr.GetOrCreatePlugin(cfg.Plugin, true)
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

	c, closeSource, err := s.plg.Source(buffer, s.cfg.Config)
	s.close = closeSource

	if err != nil {
		return nil, fmt.Errorf("failed to start plugin source: %w", err)
	}

	res := make(chan *message.RunnerMessage, buffer)
	go func() {
		defer close(res)
		for m := range c {
			res <- message.NewRunnerMessage(m)
		}
	}()

	return res, nil
}

func (s *PluginSource) Close() error {
	if s.close != nil {
		s.close()
	}
	return nil
}
