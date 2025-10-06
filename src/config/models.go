package config

import (
	"github.com/sandrolain/events-bridge/src/connectors"
)

type EnvConfig struct {
	ConfigFilePath string `env:"EB_CONFIG_FILE_PATH" default:"/etc/events-bridge/config.yaml" validate:"omitempty,filepath"`
	// Optional: raw configuration content (YAML or JSON). If set, it takes precedence over ConfigFilePath.
	ConfigContent string `env:"EB_CONFIG_CONTENT" validate:"omitempty"`
	// Optional: explicit config format when using ConfigContent. One of: yaml, yml, json.
	ConfigFormat string `env:"EB_CONFIG_FORMAT" validate:"omitempty,oneof=yaml yml json"`
}

type Config struct {
	Services []connectors.ServiceConfig `yaml:"services" json:"services"`
	Source   connectors.SourceConfig    `yaml:"source" json:"source" validate:"required"`
	Runners  []connectors.RunnerConfig  `yaml:"runners" json:"runners"`
	Target   connectors.TargetConfig    `yaml:"target" json:"target"`
}
