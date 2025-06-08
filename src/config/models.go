package config

import (
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/targets"
)

type EnvConfig struct {
	ConfigFilePath string `env:"CONFIG_FILE_PATH" default:"/etc/events-bridge/config.yaml" validate:"required,filepath"`
}

type Config struct {
	Source  sources.SourceConfig  `yaml:"source" json:"source" validate:"required"`
	Runner  runners.RunnerConfig  `yaml:"runner" json:"runner" validate:"required"`
	Target  targets.TargetConfig  `yaml:"target" json:"target" validate:"required"`
	Plugins []plugin.PluginConfig `yaml:"plugins" json:"plugins" validate:"dive"`
}
