package pluginrunner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
)

var _ runners.Runner = &PluginRunner{}

type PluginRunner struct {
	cfg     *runners.RunnerPluginConfig
	slog    *slog.Logger
	mgr     *plugin.PluginManager
	plg     *plugin.Plugin
	timeout time.Duration
}

// New creates a new instance of PluginRunner
func New(mgr *plugin.PluginManager, cfg *runners.RunnerPluginConfig) (runners.Runner, error) {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = runners.DefaultTimeout
	}

	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("runner plugin config cannot be nil")
	}

	plg, err := mgr.GetPlugin(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Name, err)
	}
	return &PluginRunner{
		cfg:  cfg,
		slog: slog.Default().With("context", "Plugin Runner", "id", cfg.Name),
		mgr:  mgr,
		plg:  plg,
	}, nil
}

func (p *PluginRunner) Process(msg *message.RunnerMessage) (*message.RunnerMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	return p.plg.Runner(ctx, msg)
}

func (p *PluginRunner) Close() error {
	return nil
}
