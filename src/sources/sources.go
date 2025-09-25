package sources

import (
	"time"

	"github.com/sandrolain/events-bridge/src/message"
)

const NewMethodName = "NewSource"

type Source interface {
	Produce(int) (<-chan *message.RunnerMessage, error)
	Close() error
}

const DefaultTimeout = 5 * time.Second

type SourceConfig struct {
	Type   string `yaml:"type" json:"type" validate:"required"`
	Buffer int    `yaml:"buffer" json:"buffer"`
	// Generic options passed to connector plugins. Preferred over typed fields below.
	Options map[string]any      `yaml:"options" json:"options"`
	Plugin  *SourcePluginConfig `yaml:"plugin" json:"plugin" validate:"omitempty,required_if=Type plugin"`
}
type SourcePluginConfig struct {
	Name   string            `yaml:"name" json:"name" validate:"required"`
	Config map[string]string `yaml:"config,omitempty" json:"config,omitempty"`
}
