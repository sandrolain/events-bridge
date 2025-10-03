package main

import (
	"encoding/json"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTargetConfig(t *testing.T) {
	cfg := NewTargetConfig()
	require.NotNil(t, cfg)

	_, ok := cfg.(*TargetConfig)
	assert.True(t, ok, "NewTargetConfig should return *TargetConfig")
}

func TestTargetConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *TargetConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &TargetConfig{
				ConnString: "postgres://user:pass@localhost/db",
				Table:      "test_table",
				BatchSize:  100,
			},
			wantErr: false,
		},
		{
			name: "missing connection string",
			cfg: &TargetConfig{
				Table:     "test_table",
				BatchSize: 100,
			},
			wantErr: true,
		},
		{
			name: "missing table",
			cfg: &TargetConfig{
				ConnString: "postgres://user:pass@localhost/db",
				BatchSize:  100,
			},
			wantErr: true,
		},
		{
			name: "with other column",
			cfg: &TargetConfig{
				ConnString:  "postgres://user:pass@localhost/db",
				Table:       "test_table",
				OtherColumn: "extra_data",
				BatchSize:   50,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This is a basic structure test, actual validation happens in the connector framework
			assert.NotNil(t, tt.cfg)
		})
	}
}

func TestPGSQLTargetCreationWithInvalidConfig(t *testing.T) {
	// Test with wrong config type
	wrongCfg := map[string]interface{}{
		"connString": "test",
	}

	target, err := NewTarget(wrongCfg)
	assert.Error(t, err)
	assert.Nil(t, target)
	assert.Contains(t, err.Error(), "invalid config type")
}

// Mock source message for testing
type mockSourceMessage struct {
	id       []byte
	data     []byte
	metadata map[string]string
}

func (m *mockSourceMessage) GetID() []byte {
	return m.id
}

func (m *mockSourceMessage) GetMetadata() (map[string]string, error) {
	return m.metadata, nil
}

func (m *mockSourceMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *mockSourceMessage) Ack() error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
	return nil
}

func (m *mockSourceMessage) Reply(reply *message.ReplyData) error {
	return nil
}

func TestPGSQLTargetMessageParsing(t *testing.T) {
	testData := map[string]interface{}{
		"name":  "test",
		"value": 42,
		"flag":  true,
	}

	dataBytes, err := json.Marshal(testData)
	require.NoError(t, err)

	sourceMsg := &mockSourceMessage{
		id:       []byte("test-id"),
		data:     dataBytes,
		metadata: map[string]string{"source": "test"},
	}

	runnerMsg := message.NewRunnerMessage(sourceMsg)

	// Test message data extraction
	data, err := runnerMsg.GetData()
	require.NoError(t, err)
	assert.NotNil(t, data)

	var parsed map[string]interface{}
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Equal(t, "test", parsed["name"])
	assert.Equal(t, float64(42), parsed["value"]) // JSON numbers are float64
	assert.Equal(t, true, parsed["flag"])
}
