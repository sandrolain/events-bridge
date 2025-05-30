package models

import (
	"time"

	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/targets"
)

type EnvConfig struct {
	ConfigFilePath string `env:"CONFIG_FILE_PATH" default:"/etc/events-bridge/config.yaml" validate:"required,filepath"`
}

type Config struct {
	Source sources.SourceConfig `yaml:"source" json:"source" validate:"required"`
	Runner RunnerConfig         `yaml:"runner" json:"runner" validate:"required"`
	Target targets.TargetConfig `yaml:"target" json:"target" validate:"required"`
}

type RunnerConfig struct {
	Type RunnerType        `yaml:"type" json:"type" validate:"required,oneof=wasm es5"`
	WASM *RunnerWASMConfig `yaml:"wasm" json:"wasm"`
	ES5  *RunnerES5Config  `yaml:"es5" json:"es5"`
}

type RunnerWASMConfig struct {
	Path     string        `yaml:"path" json:"module_path" validate:"required,filepath"`
	Function string        `yaml:"function" json:"function" validate:"required"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout" validate:"required"`
}

type RunnerES5Config struct {
	Path    string        `yaml:"path" json:"path" validate:"omitempty,filepath"`
	Timeout time.Duration `yaml:"timeout" json:"timeout" validate:"required"`
}
