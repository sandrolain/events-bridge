package pluginrunner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/utils"
)

var _ runners.Runner = &PluginRunner{}

type Config struct {
	Name    string
	Timeout time.Duration
}

type PluginRunner struct {
	cfg     *Config
	slog    *slog.Logger
	mgr     *plugin.PluginManager
	plg     *plugin.Plugin
	timeout time.Duration
}

func parseConfig(opts map[string]any) (*Config, error) {
	parser := &utils.OptsParser{}
	name := parser.OptString(opts, "name", "", utils.StringNonEmpty())
	timeout := parser.OptDuration(opts, "timeout", runners.DefaultTimeout)
	if err := parser.Error(); err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("plugin runner name is required")
	}
	if timeout <= 0 {
		timeout = runners.DefaultTimeout
	}
	return &Config{
		Name:    name,
		Timeout: timeout,
	}, nil
}

// New creates a new instance of PluginRunner
func New(mgr *plugin.PluginManager, opts map[string]any) (runners.Runner, error) {
	if mgr == nil {
		return nil, fmt.Errorf("plugin manager cannot be nil")
	}
	cfg, err := parseConfig(opts)
	if err != nil {
		return nil, err
	}

	plg, err := mgr.GetPlugin(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("cannot get plugin %s: %w", cfg.Name, err)
	}
	return &PluginRunner{
		cfg:     cfg,
		slog:    slog.Default().With("context", "Plugin Runner", "id", cfg.Name),
		mgr:     mgr,
		plg:     plg,
		timeout: cfg.Timeout,
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
