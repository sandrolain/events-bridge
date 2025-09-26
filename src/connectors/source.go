package connectors

import (
	"github.com/sandrolain/events-bridge/src/message"
)

const NewSourceMethodName = "NewSource"
const NewSourceConfigName = "NewSourceConfig"

type Source interface {
	Produce(int) (<-chan *message.RunnerMessage, error)
	Close() error
}

type SourceConfig struct {
	Type   string `yaml:"type" json:"type" validate:"required"`
	Buffer int    `yaml:"buffer" json:"buffer"`
	// Generic options passed to connector plugins. Preferred over typed fields below.
	Options map[string]any `yaml:"options" json:"options"`
}
