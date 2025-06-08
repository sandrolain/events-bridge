package runners

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

type Runner interface {
	Ingest(<-chan message.Message) (<-chan message.Message, error)
	Close() error
}

type RunnerType string

const (
	RunnerTypeNone   RunnerType = "none"
	RunnerTypeWASM   RunnerType = "wasm"
	RunnerTypeES5    RunnerType = "es5"
	RunnerTypePlugin RunnerType = "plugin"
	RunnerTypeCLI    RunnerType = "cli"
)

type RunnerConfig struct {
	Type   RunnerType          `yaml:"type" json:"type" validate:"required,oneof=wasm es5 plugin cli"`
	WASM   *RunnerWASMConfig   `yaml:"wasm" json:"wasm"`
	ES5    *RunnerES5Config    `yaml:"es5" json:"es5"`
	Plugin *RunnerPluginConfig `yaml:"plugin" json:"plugin"`
	CLI    *RunnerCLIConfig    `yaml:"cli" json:"cli"`
}

type RunnerCLIConfig struct {
	Command string            `yaml:"command" json:"command" validate:"required"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
	Args    []string          `yaml:"args" json:"args"`
	Envs    map[string]string `yaml:"envs" json:"envs"`
}

type RunnerPluginConfig struct {
	Name string `yaml:"name" json:"name" validate:"required"`
}

type RunnerWASMConfig struct {
	Path    string        `yaml:"path" json:"module_path" validate:"required,filepath"`
	Timeout time.Duration `yaml:"timeout" json:"timeout" `
}

type RunnerES5Config struct {
	Path    string        `yaml:"path" json:"path" validate:"omitempty,filepath"`
	Timeout time.Duration `yaml:"timeout" json:"timeout" `
}
