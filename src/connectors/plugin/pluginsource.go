package pluginconn

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/sources"
)

type PluginSource struct {
	config  *sources.SourcePluginConfig
	slog    *slog.Logger
	mgr     *plugin.PluginManager
	plg     *plugin.Plugin
	c       <-chan *message.RunnerMessage
	timeout time.Duration
	close   func()
}

func NewSource(mgr *plugin.PluginManager, cfg *sources.SourcePluginConfig) (sources.Source, error) {
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

func (s *PluginSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.slog.Info("starting plugin source", "id", s.config.Name)

	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	var err error
	s.c, s.close, err = s.plg.Source(ctx, buffer, s.config.Config)
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
