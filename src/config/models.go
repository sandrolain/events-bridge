package config

import (
	"github.com/sandrolain/events-bridge/src/plugin"
	"github.com/sandrolain/events-bridge/src/runners"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/targets"
)

type EnvConfig struct {
	ConfigFilePath string `env:"CONFIG_FILE_PATH" default:"/etc/events-bridge/config.yaml" validate:"omitempty,filepath"`
	// Optional: raw configuration content (YAML or JSON). If set, it takes precedence over ConfigFilePath.
	ConfigContent string `env:"CONFIG_CONTENT" validate:"omitempty"`
	// Optional: explicit config format when using ConfigContent. One of: yaml, yml, json.
	ConfigFormat string `env:"CONFIG_FORMAT" validate:"omitempty,oneof=yaml yml json"`
}

type Config struct {
	Source  sources.SourceConfig  `yaml:"source" json:"source" validate:"required"`
	Runner  runners.RunnerConfig  `yaml:"runner" json:"runner"`
	Target  targets.TargetConfig  `yaml:"target" json:"target"`
	Plugins []plugin.PluginConfig `yaml:"plugins" json:"plugins" validate:"dive"`
}
