package runners

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const NewMethodName = "New"

type Runner interface {
	Process(*message.RunnerMessage) (*message.RunnerMessage, error)
	Close() error
}

type RunnerType string

const (
	RunnerTypeNone      RunnerType = "none"
	RunnerTypeWASM      RunnerType = "wasm"
	RunnerTypeES5       RunnerType = "es5"
	RunnerTypePlugin    RunnerType = "plugin"
	RunnerTypeCLI       RunnerType = "cli"
	RunnerTypeJSONLogic RunnerType = "jsonlogic"
	RunnerTypeGPT       RunnerType = "gpt"
)

type RunnerConfig struct {
	Type      RunnerType             `yaml:"type" json:"type" validate:"required,oneof=wasm es5 plugin cli gpt jsonlogic"`
	Routines  int                    `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	WASM      *RunnerWASMConfig      `yaml:"wasm" json:"wasm"`
	ES5       *RunnerES5Config       `yaml:"es5" json:"es5"`
	Plugin    *RunnerPluginConfig    `yaml:"plugin" json:"plugin"`
	CLI       *RunnerCLIConfig       `yaml:"cli" json:"cli"`
	JSONLogic *RunnerJSONLogicConfig `yaml:"jsonlogic" json:"jsonlogic"`
	GPT       *RunnerGPTRunnerConfig `yaml:"gpt" json:"gpt"`
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

type RunnerJSONLogicConfig struct {
	Path    string        `yaml:"path" json:"path" validate:"required,filepath"`
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
}

type RunnerGPTRunnerConfig struct {
	ApiURL    string        `yaml:"api_url" json:"api_url" validate:"url,omitempty"`
	ApiKey    string        `yaml:"api_key" json:"api_key"`
	Action    string        `yaml:"action" json:"action" validate:"required"`
	Model     string        `yaml:"model" json:"model"`
	BatchSize int           `yaml:"batch_size" json:"batch_size"`
	BatchWait time.Duration `yaml:"batch_wait" json:"batch_wait"`
	MaxTokens int           `yaml:"max_tokens" json:"max_tokens"`
	Timeout   time.Duration `yaml:"timeout" json:"timeout"`
}
