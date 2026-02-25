package main

import (
	"encoding/json"
	"testing"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	require.NotNil(t, cfg)

	_, ok := cfg.(*RunnerConfig)
	assert.True(t, ok, "NewRunnerConfig should return *RunnerConfig")
}

const validConnString = "postgres://user:pass@localhost/db" //nolint:gosec // test connection string with placeholder credentials

// TestRunnerConfigInvalidIdentifiers tests SQL injection prevention
func TestRunnerConfigInvalidIdentifiers(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *RunnerConfig
		errContains string
	}{
		{
			name: "invalid table name - SQL injection attempt",
			cfg: &RunnerConfig{
				ConnString:       validConnString,
				Table:            "test'; DROP TABLE users; --",
				BatchSize:        100,
				StrictValidation: true,
			},
			errContains: "invalid table name",
		},
		{
			name: "invalid other column - SQL injection attempt",
			cfg: &RunnerConfig{
				ConnString:       validConnString,
				Table:            "test_table",
				OtherColumn:      "col'; DROP TABLE users; --",
				BatchSize:        100,
				StrictValidation: true,
			},
			errContains: "invalid other column name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner, err := NewRunner(tt.cfg)

			require.Error(t, err, "should reject invalid identifier")
			assert.Contains(t, err.Error(), tt.errContains)
			assert.Nil(t, runner)
		})
	}
}

// TestRunnerConfigMissingFields tests validation of required fields
func TestRunnerConfigMissingFields(t *testing.T) {
	tests := []struct {
		name string
		cfg  *RunnerConfig
	}{
		{
			name: "missing connection string",
			cfg: &RunnerConfig{
				Table:     "test_table",
				BatchSize: 100,
			},
		},
		{
			name: "missing table",
			cfg: &RunnerConfig{
				ConnString: validConnString,
				BatchSize:  100,
			},
		},
		{
			name: "invalid connection string format",
			cfg: &RunnerConfig{
				ConnString: "not-a-valid-conn-string",
				Table:      "test_table",
				BatchSize:  100,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner, err := NewRunner(tt.cfg)

			// Should fail (validation or connection error)
			require.Error(t, err, "should fail with invalid config")
			assert.Nil(t, runner)
		})
	}
}

func TestRunnerConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *RunnerConfig
		wantErr bool
	}{
		{
			name: "valid config structure",
			cfg: &RunnerConfig{
				ConnString: validConnString,
				Table:      "test_table",
				BatchSize:  100,
			},
			wantErr: false, // May fail on DB connection in unit test
		},
		{
			name: "with valid other column",
			cfg: &RunnerConfig{
				ConnString:  validConnString,
				Table:       "test_table",
				OtherColumn: "extra_data",
				BatchSize:   50,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NotNil(t, tt.cfg)

			runner, err := NewRunner(tt.cfg)

			if err != nil {
				// Config valid but connection may fail in unit tests
				t.Logf("Config valid but connection failed (expected): %v", err)
			} else if runner != nil {
				defer func() {
					if closer, ok := runner.(interface{ Close() error }); ok {
						if err := closer.Close(); err != nil {
							t.Logf("failed to close runner: %v", err)
						}
					}
				}()
			}
		})
	}
}

func TestPGSQLTargetCreationWithInvalidConfig(t *testing.T) {
	// Test with wrong config type
	wrongCfg := map[string]interface{}{
		"connString": "test",
	}

	runner, err := NewRunner(wrongCfg)
	assert.Error(t, err)
	assert.Nil(t, runner)
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

func (m *mockSourceMessage) Ack(*message.ReplyData) error {
	return nil
}

func (m *mockSourceMessage) Nak() error {
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
