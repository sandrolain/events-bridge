package pluginsource

import (
	"fmt"
	"log/slog"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/sources/source"
)

type SourcePluginConfig struct {
	ID     string            `yaml:"id" json:"id" validate:"required"`
	Config map[string]string `yaml:"config,omitempty" json:"config,omitempty"`
}

type PluginSource struct {
	config  *SourcePluginConfig
	slog    *slog.Logger
	mgr     *plugin.PluginManager
	plg     *plugin.Plugin
	c       chan message.Message
	started bool
}

func New(mgr *plugin.PluginManager, cfg *SourcePluginConfig) (source.Source, error) {
	plg, err := mgr.GetPlugin(cfg.ID)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.ID, err)
	}

	ps := &PluginSource{
		config: cfg,
		slog:   slog.Default().With("context", "PLUGIN"),
		mgr:    mgr,
		plg:    plg,
	}
	return ps, nil
}

func (s *PluginSource) Produce(buffer int) (<-chan message.Message, error) {
	if s.started {
		return s.c, nil
	}
	err := s.plg.Start()
	if err != nil {
		return nil, fmt.Errorf("cannot start plugin %s: %w", s.config.ID, err)
	}
	s.slog.Info("starting plugin source", "id", s.config.ID)
	c, err := s.plg.Source(buffer, s.config.Config)
	if err != nil {
		return nil, err
	}
	s.c = nil // non serve assegnare c, è già il canale restituito dal plugin
	s.started = true
	return c, nil
}

func (s *PluginSource) Close() error {
	if s.c != nil {
		close(s.c)
	}
	if s.plg != nil {
		s.plg.Stop()
	}
	return nil
}
