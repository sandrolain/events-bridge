package plugin

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

var globManager *PluginManager

func GetPluginManager() (res *PluginManager, err error) {
	if globManager != nil {
		return globManager, nil
	}

	res, err = NewPluginManager()
	if err != nil {
		return nil, fmt.Errorf("cannot create plugin manager: %w", err)
	}

	globManager = res
	return res, nil
}

func NewPluginManager() (res *PluginManager, err error) {

	l := slog.Default().With("context", "Plugin Manager")

	l.Info("starting plugin manager")

	res = &PluginManager{
		slog:    l,
		plugins: make(map[string]*Plugin),
	}
	return
}

var DefaultTimeout = 5 * time.Second

type PluginManager struct {
	slog    *slog.Logger
	plugins map[string]*Plugin
	server  *grpc.Server
}

func (p *PluginManager) Stop() (err error) {
	for _, plugin := range p.plugins {
		plugin.Stop()
	}
	p.server.Stop()
	return
}

func (p *PluginManager) CreatePlugin(cfg PluginConfig) (plg *Plugin, err error) {
	p.slog.Info("creating plugin", "id", cfg.Name)

	_, ok := p.plugins[cfg.Name]
	if ok {
		err = fmt.Errorf("plugin with ID %s already exists", cfg.Name)
		return
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	id := uuid.New().String()

	p.slog.Info("creating plugin", "id", id, "protocol", cfg.Protocol)

	plg = &Plugin{
		Config:  cfg,
		ID:      id,
		slog:    p.slog.With("plugin", cfg.Name, "id", id),
		timeout: timeout,
	}
	p.plugins[cfg.Name] = plg

	return
}

func (p *PluginManager) GetPlugin(id string) (res *Plugin, err error) {
	res, ok := p.plugins[id]
	if !ok {
		err = fmt.Errorf("plugin not found")
	}
	return
}
