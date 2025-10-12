package main

import (
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// SASLConfig holds SASL authentication configuration for Kafka.
// Supports PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 mechanisms.
type SASLConfig struct {
	// Enabled enables SASL authentication.
	Enabled bool `mapstructure:"enabled" default:"false"`

	// Mechanism specifies the SASL mechanism to use.
	// Supported values: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"
	// PLAIN: Simple username/password (requires TLS in production)
	// SCRAM-SHA-256: Salted Challenge Response Authentication (recommended)
	// SCRAM-SHA-512: SCRAM with SHA-512 (most secure)
	Mechanism string `mapstructure:"mechanism" validate:"required_if=Enabled true,omitempty,oneof=PLAIN SCRAM-SHA-256 SCRAM-SHA-512"`

	// Username for SASL authentication.
	Username string `mapstructure:"username" validate:"required_if=Enabled true"`

	// Password for SASL authentication.
	// WARNING: Consider using environment variables or secret managers for production.
	Password string `mapstructure:"password" validate:"required_if=Enabled true"`
}

// BuildSASLMechanism creates a SASL mechanism for Kafka authentication.
// Returns nil if SASL is not enabled.
func (c *SASLConfig) BuildSASLMechanism() (sasl.Mechanism, error) {
	if !c.Enabled {
		return nil, nil
	}

	if c.Username == "" || c.Password == "" {
		return nil, fmt.Errorf("username and password are required for SASL authentication")
	}

	switch c.Mechanism {
	case "PLAIN":
		return plain.Mechanism{
			Username: c.Username,
			Password: c.Password,
		}, nil

	case "SCRAM-SHA-256":
		mechanism, err := scram.Mechanism(scram.SHA256, c.Username, c.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-256 mechanism: %w", err)
		}
		return mechanism, nil

	case "SCRAM-SHA-512":
		mechanism, err := scram.Mechanism(scram.SHA512, c.Username, c.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-512 mechanism: %w", err)
		}
		return mechanism, nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", c.Mechanism)
	}
}
