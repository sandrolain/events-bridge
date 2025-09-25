package targets

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const NewMethodName = "NewTarget"

type Target interface {
	Consume(*message.RunnerMessage) error
	Close() error
}

var DefaultTimeout = 5 * time.Second

type TargetConfig struct {
	Type     string              `yaml:"type" json:"type" validate:"required"`
	Buffer   int                 `yaml:"buffer" json:"buffer"`
	Routines int                 `yaml:"routines" json:"routines" validate:"omitempty,min=1"`
	Options  map[string]any      `yaml:"options" json:"options"`
	Plugin   *TargetPluginConfig `yaml:"plugin" json:"plugin" validate:"omitempty,required_if=Type plugin"`
}

type TargetPluginConfig struct {
	Name    string            `yaml:"name" json:"name" validate:"required"`
	Timeout time.Duration     `yaml:"timeout" json:"timeout"`
	Config  map[string]string `yaml:"config,omitempty" json:"config,omitempty"`
}
