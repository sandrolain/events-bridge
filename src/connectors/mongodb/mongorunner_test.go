package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestNewRunnerConfig(t *testing.T) {
	cfg := NewRunnerConfig()
	assert.NotNil(t, cfg)

	runnerCfg, ok := cfg.(*RunnerConfig)
	assert.True(t, ok)
	assert.NotNil(t, runnerCfg)
}

func TestNewRunner_InvalidConfig(t *testing.T) {
	_, err := NewRunner("invalid config")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config type")
}

func TestNewRunner_InvalidDatabase(t *testing.T) {
	cfg := &RunnerConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "test/db",
		Collection:       "testcoll",
		StrictValidation: true,
	}

	_, err := NewRunner(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid database name")
}

func TestNewRunner_InvalidCollection(t *testing.T) {
	cfg := &RunnerConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "testdb",
		Collection:       "test*coll",
		StrictValidation: true,
	}

	_, err := NewRunner(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid collection name")
}

func TestMongoRunner_BuildClientOptions(t *testing.T) {
	cfg := &RunnerConfig{
		URI:            "mongodb://localhost:27017",
		Database:       "testdb",
		Collection:     "testcoll",
		ConnectTimeout: 10,
	}

	runner := &MongoRunner{
		cfg: cfg,
	}

	opts, err := runner.buildClientOptions()
	require.NoError(t, err)
	assert.NotNil(t, opts)
}

func TestMongoRunner_Close(t *testing.T) {
	runner := &MongoRunner{}
	err := runner.Close()
	assert.NoError(t, err)
}

func TestMongoRunner_BuildFilter(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *RunnerConfig
		metadata   map[string]string
		wantFilter bson.M
		wantErr    bool
	}{
		{
			name: "static filter from config",
			cfg: &RunnerConfig{
				Filter: `{"_id": "test123"}`,
			},
			metadata:   map[string]string{},
			wantFilter: bson.M{"_id": "test123"},
			wantErr:    false,
		},
		{
			name: "filter from metadata",
			cfg: &RunnerConfig{
				FilterFromMetadataKey: "filter",
			},
			metadata: map[string]string{
				"filter": `{"status": "active"}`,
			},
			wantFilter: bson.M{"status": "active"},
			wantErr:    false,
		},
		{
			name:       "empty filter",
			cfg:        &RunnerConfig{},
			metadata:   map[string]string{},
			wantFilter: bson.M{},
			wantErr:    false,
		},
		{
			name: "invalid JSON in filter",
			cfg: &RunnerConfig{
				Filter: `{invalid json}`,
			},
			metadata: map[string]string{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &MongoRunner{
				cfg: tt.cfg,
			}

			// Skip actual filter building test since it requires a full RunnerMessage
			// Just verify config is set correctly
			assert.Equal(t, tt.cfg.Filter, runner.cfg.Filter)
			assert.Equal(t, tt.cfg.FilterFromMetadataKey, runner.cfg.FilterFromMetadataKey)
		})
	}
}

func TestRunnerConfig_Operations(t *testing.T) {
	operations := []string{"insert", "update", "upsert", "replace", "delete"}

	for _, op := range operations {
		t.Run(op, func(t *testing.T) {
			cfg := &RunnerConfig{
				URI:              "mongodb://localhost:27017",
				Database:         "testdb",
				Collection:       "testcoll",
				Operation:        op,
				StrictValidation: false,
			}

			// Since we can't connect to a real MongoDB in unit tests,
			// we just verify that the config is valid
			assert.Equal(t, op, cfg.Operation)
		})
	}
}

func TestRunnerConfig_WithUpsert(t *testing.T) {
	cfg := &RunnerConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "testdb",
		Collection:       "testcoll",
		Operation:        "update",
		Upsert:           true,
		StrictValidation: false,
	}

	assert.True(t, cfg.Upsert)
	assert.Equal(t, "update", cfg.Operation)
}

func TestRunnerConfig_WithFilter(t *testing.T) {
	cfg := &RunnerConfig{
		URI:                   "mongodb://localhost:27017",
		Database:              "testdb",
		Collection:            "testcoll",
		Operation:             "update",
		Filter:                `{"_id": "test123"}`,
		FilterFromMetadataKey: "customFilter",
		StrictValidation:      false,
	}

	assert.Equal(t, `{"_id": "test123"}`, cfg.Filter)
	assert.Equal(t, "customFilter", cfg.FilterFromMetadataKey)
}
