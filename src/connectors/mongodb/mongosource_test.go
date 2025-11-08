package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		strict   bool
		wantErr  bool
		errMatch string
	}{
		{
			name:    "valid identifier",
			input:   "test_collection",
			strict:  true,
			wantErr: false,
		},
		{
			name:    "valid with hyphen",
			input:   "test-collection",
			strict:  true,
			wantErr: false,
		},
		{
			name:     "empty identifier",
			input:    "",
			strict:   true,
			wantErr:  true,
			errMatch: "cannot be empty",
		},
		{
			name:     "too long",
			input:    "a" + string(make([]byte, 65)),
			strict:   true,
			wantErr:  true,
			errMatch: "exceeds maximum length",
		},
		{
			name:     "invalid character in strict mode",
			input:    "test.collection",
			strict:   true,
			wantErr:  true,
			errMatch: "invalid identifier",
		},
		{
			name:    "valid in non-strict mode",
			input:   "test.collection",
			strict:  false,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentifier(tt.input, tt.strict)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMatch != "" {
					assert.Contains(t, err.Error(), tt.errMatch)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewSourceConfig(t *testing.T) {
	cfg := NewSourceConfig()
	assert.NotNil(t, cfg)

	sourceCfg, ok := cfg.(*SourceConfig)
	assert.True(t, ok)
	assert.NotNil(t, sourceCfg)
}

func TestNewSource_InvalidConfig(t *testing.T) {
	_, err := NewSource("invalid config")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid config type")
}

func TestNewSource_ValidConfig(t *testing.T) {
	cfg := &SourceConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "testdb",
		Collection:       "testcoll",
		FullDocument:     "updateLookup",
		StrictValidation: true,
	}

	source, err := NewSource(cfg)
	require.NoError(t, err)
	assert.NotNil(t, source)

	mongoSource, ok := source.(*MongoSource)
	assert.True(t, ok)
	assert.Equal(t, cfg, mongoSource.cfg)
}

func TestNewSource_InvalidDatabase(t *testing.T) {
	cfg := &SourceConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "test/db",
		Collection:       "testcoll",
		StrictValidation: true,
	}

	_, err := NewSource(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid database name")
}

func TestNewSource_InvalidCollection(t *testing.T) {
	cfg := &SourceConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "testdb",
		Collection:       "test\\coll",
		StrictValidation: true,
	}

	_, err := NewSource(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid collection name")
}

func TestMongoSource_BuildClientOptions(t *testing.T) {
	cfg := &SourceConfig{
		URI:            "mongodb://localhost:27017",
		Database:       "testdb",
		Collection:     "testcoll",
		ConnectTimeout: 10,
	}

	source := &MongoSource{
		cfg: cfg,
	}

	opts, err := source.buildClientOptions()
	require.NoError(t, err)
	assert.NotNil(t, opts)
}

func TestMongoSource_Close(t *testing.T) {
	source := &MongoSource{}
	err := source.Close()
	assert.NoError(t, err)
}

func TestSourceConfig_WithPipeline(t *testing.T) {
	cfg := &SourceConfig{
		URI:              "mongodb://localhost:27017",
		Database:         "testdb",
		Collection:       "testcoll",
		StrictValidation: true,
		Pipeline: []bson.M{
			{"$match": bson.M{"operationType": "insert"}},
		},
	}

	source, err := NewSource(cfg)
	require.NoError(t, err)
	assert.NotNil(t, source)

	mongoSource := source.(*MongoSource)
	assert.Len(t, mongoSource.cfg.Pipeline, 1)
}

func TestSourceConfig_WithResumeToken(t *testing.T) {
	resumeToken := bson.M{"_data": "test-token"}
	timestamp := primitive.Timestamp{T: 1234567890, I: 1}

	cfg := &SourceConfig{
		URI:                  "mongodb://localhost:27017",
		Database:             "testdb",
		Collection:           "testcoll",
		StrictValidation:     true,
		ResumeAfter:          resumeToken,
		StartAtOperationTime: &timestamp,
	}

	source, err := NewSource(cfg)
	require.NoError(t, err)
	assert.NotNil(t, source)

	mongoSource := source.(*MongoSource)
	assert.Equal(t, resumeToken, mongoSource.cfg.ResumeAfter)
	assert.Equal(t, &timestamp, mongoSource.cfg.StartAtOperationTime)
}
