package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// RunnerConfig defines the configuration for MongoDB runner connector
type RunnerConfig struct {
	// MongoDB connection URI
	// Format: mongodb://username:password@host:port/database
	// For MongoDB Atlas: mongodb+srv://username:password@cluster.mongodb.net/database
	// For security, use environment variables or secret managers for credentials
	URI string `mapstructure:"uri" validate:"required"`

	// Target database name
	Database string `mapstructure:"database" validate:"required"`

	// Target collection name
	Collection string `mapstructure:"collection" validate:"required"`

	// TLS configuration for encrypted connections
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// Operation type: "insert", "update", "upsert", "replace", "delete"
	Operation string `mapstructure:"operation" default:"insert" validate:"oneof=insert update upsert replace delete"`

	// Filter for update/delete operations (JSON string or metadata key)
	Filter string `mapstructure:"filter"`

	// Metadata key to extract filter dynamically
	FilterFromMetadataKey string `mapstructure:"filterFromMetadataKey"`

	// Upsert option for update operations
	Upsert bool `mapstructure:"upsert" default:"false"`

	// Connection timeout
	ConnectTimeout time.Duration `mapstructure:"connectTimeout" default:"10s" validate:"gt=0"`

	// Operation timeout
	OperationTimeout time.Duration `mapstructure:"operationTimeout" default:"5s" validate:"gt=0"`

	// Enable strict identifier validation (recommended: true)
	StrictValidation bool `mapstructure:"strictValidation" default:"true"`
}

type MongoRunner struct {
	cfg        *RunnerConfig
	slog       *slog.Logger
	client     *mongo.Client
	collection *mongo.Collection
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

// buildClientOptions creates MongoDB client options with TLS and timeout
func (t *MongoRunner) buildClientOptions() (*options.ClientOptions, error) {
	// Resolve URI in case it contains secrets
	resolvedURI, err := secrets.Resolve(t.cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve URI: %w", err)
	}

	clientOpts := options.Client().ApplyURI(resolvedURI)

	// Apply TLS configuration
	tlsConf, err := tlsconfig.BuildClientConfigIfEnabled(t.cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	if tlsConf != nil {
		clientOpts.SetTLSConfig(tlsConf)
	}

	// Set connect timeout
	clientOpts.SetConnectTimeout(t.cfg.ConnectTimeout)

	return clientOpts, nil
}

// NewRunner creates a MongoDB runner from configuration
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate database and collection names
	if err := validateIdentifier(cfg.Database, cfg.StrictValidation); err != nil {
		return nil, fmt.Errorf("invalid database name: %w", err)
	}
	if err := validateIdentifier(cfg.Collection, cfg.StrictValidation); err != nil {
		return nil, fmt.Errorf("invalid collection name: %w", err)
	}

	target := &MongoRunner{
		cfg:  cfg,
		slog: slog.Default().With("context", "MongoDB Runner"),
	}

	// Build client options
	clientOpts, err := target.buildClientOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to build client options: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()

	// Create MongoDB client
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Test connection
	if err := client.Ping(ctx, nil); err != nil {
		// Disconnect on error
		if disconnectErr := client.Disconnect(ctx); disconnectErr != nil {
			target.slog.Warn("failed to disconnect MongoDB client after ping error", "err", disconnectErr)
		}
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	target.client = client
	target.collection = client.Database(cfg.Database).Collection(cfg.Collection)

	tlsEnabled := cfg.TLS != nil && cfg.TLS.Enabled

	target.slog.Info("MongoDB runner connected",
		"database", cfg.Database,
		"collection", cfg.Collection,
		"operation", cfg.Operation,
		"tls", tlsEnabled,
		"strictValidation", cfg.StrictValidation,
	)

	return target, nil
}

func (t *MongoRunner) Process(msg *message.RunnerMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.OperationTimeout)
	defer cancel()

	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get message data: %w", err)
	}

	// Parse data as BSON document
	var document bson.M
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("failed to unmarshal message data: %w", err)
	}

	// Get metadata
	metadata, err := msg.GetMetadata()
	if err != nil {
		return fmt.Errorf("failed to get message metadata: %w", err)
	}

	// Add metadata to document if not already present
	for k, v := range metadata {
		if _, exists := document[k]; !exists {
			document[k] = v
		}
	}

	t.slog.Debug("processing MongoDB operation",
		"operation", t.cfg.Operation,
		"database", t.cfg.Database,
		"collection", t.cfg.Collection,
	)

	switch t.cfg.Operation {
	case "insert":
		return t.insert(ctx, document)
	case "update":
		return t.update(ctx, document, msg)
	case "upsert":
		return t.upsert(ctx, document, msg)
	case "replace":
		return t.replace(ctx, document, msg)
	case "delete":
		return t.delete(ctx, msg)
	default:
		return fmt.Errorf("unsupported operation: %s", t.cfg.Operation)
	}
}

func (t *MongoRunner) insert(ctx context.Context, document bson.M) error {
	result, err := t.collection.InsertOne(ctx, document)
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}

	t.slog.Debug("document inserted", "insertedID", result.InsertedID)
	return nil
}

func (t *MongoRunner) update(ctx context.Context, document bson.M, msg *message.RunnerMessage) error {
	filter, err := t.buildFilter(msg)
	if err != nil {
		return fmt.Errorf("failed to build filter: %w", err)
	}

	update := bson.M{"$set": document}

	opts := options.Update().SetUpsert(t.cfg.Upsert)
	result, err := t.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("failed to update document: %w", err)
	}

	t.slog.Debug("document updated",
		"matched", result.MatchedCount,
		"modified", result.ModifiedCount,
		"upserted", result.UpsertedCount,
	)
	return nil
}

func (t *MongoRunner) upsert(ctx context.Context, document bson.M, msg *message.RunnerMessage) error {
	filter, err := t.buildFilter(msg)
	if err != nil {
		return fmt.Errorf("failed to build filter: %w", err)
	}

	opts := options.Replace().SetUpsert(true)
	result, err := t.collection.ReplaceOne(ctx, filter, document, opts)
	if err != nil {
		return fmt.Errorf("failed to upsert document: %w", err)
	}

	t.slog.Debug("document upserted",
		"matched", result.MatchedCount,
		"modified", result.ModifiedCount,
		"upserted", result.UpsertedCount,
	)
	return nil
}

func (t *MongoRunner) replace(ctx context.Context, document bson.M, msg *message.RunnerMessage) error {
	filter, err := t.buildFilter(msg)
	if err != nil {
		return fmt.Errorf("failed to build filter: %w", err)
	}

	opts := options.Replace().SetUpsert(t.cfg.Upsert)
	result, err := t.collection.ReplaceOne(ctx, filter, document, opts)
	if err != nil {
		return fmt.Errorf("failed to replace document: %w", err)
	}

	t.slog.Debug("document replaced",
		"matched", result.MatchedCount,
		"modified", result.ModifiedCount,
	)
	return nil
}

func (t *MongoRunner) delete(ctx context.Context, msg *message.RunnerMessage) error {
	filter, err := t.buildFilter(msg)
	if err != nil {
		return fmt.Errorf("failed to build filter: %w", err)
	}

	result, err := t.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}

	t.slog.Debug("document deleted", "deleted", result.DeletedCount)
	return nil
}

func (t *MongoRunner) buildFilter(msg *message.RunnerMessage) (bson.M, error) {
	var filter bson.M

	// Try to get filter from metadata first
	if t.cfg.FilterFromMetadataKey != "" {
		metadata, err := msg.GetMetadata()
		if err != nil {
			return nil, fmt.Errorf("failed to get metadata: %w", err)
		}

		if filterValue, exists := metadata[t.cfg.FilterFromMetadataKey]; exists {
			// Use UnmarshalExtJSON to properly handle MongoDB Extended JSON (e.g., {"$oid": "..."})
			if err := bson.UnmarshalExtJSON([]byte(filterValue), false, &filter); err != nil {
				return nil, fmt.Errorf("failed to unmarshal filter from metadata: %w", err)
			}
			return filter, nil
		}
	}

	// Otherwise use static filter from config
	if t.cfg.Filter != "" {
		// Use UnmarshalExtJSON to properly handle MongoDB Extended JSON (e.g., {"$oid": "..."})
		if err := bson.UnmarshalExtJSON([]byte(t.cfg.Filter), false, &filter); err != nil {
			return nil, fmt.Errorf("failed to unmarshal filter: %w", err)
		}
		return filter, nil
	}

	// If no filter provided, return empty filter (matches all)
	return bson.M{}, nil
}

func (t *MongoRunner) Close() error {
	if t.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := t.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("error disconnecting MongoDB client: %w", err)
		}
	}
	return nil
}
