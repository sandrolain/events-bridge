package pluginsource

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/sources/source"
)

type SourcePluginConfig struct {
	Name   string            `yaml:"name" json:"name" validate:"required"`
	Config map[string]string `yaml:"config,omitempty" json:"config,omitempty"`
}

type PluginSource struct {
	config *SourcePluginConfig
	slog   *slog.Logger
	mgr    *plugin.PluginManager
	plg    *plugin.Plugin
	c      <-chan message.Message
	close  func()
}

func New(mgr *plugin.PluginManager, cfg *SourcePluginConfig) (source.Source, error) {
	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("plugin source config cannot be nil")
	}

	plg, err := mgr.GetPlugin(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Name, err)
	}

	ps := &PluginSource{
		config: cfg,
		slog:   slog.Default().With("context", "Plugin Source", "name", cfg.Name),
		mgr:    mgr,
		plg:    plg,
	}
	return ps, nil
}

func (s *PluginSource) Produce(buffer int) (<-chan message.Message, error) {
	s.slog.Info("starting plugin source", "id", s.config.Name)
	c, closeFn, err := s.plg.Source(buffer, s.config.Config)
	if err != nil {
		return nil, err
	}
	s.c = c
	s.close = closeFn
	return c, nil
}

func (s *PluginSource) Close() error {
	if s.close != nil {
		s.close()
	}
	return nil
}
