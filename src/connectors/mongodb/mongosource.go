package main

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/sandrolain/events-bridge/src/common/secrets"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// identifierRegex validates MongoDB database and collection names
// MongoDB names can contain alphanumeric, underscore, and hyphen
var identifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// SourceConfig defines the configuration for MongoDB source connector
type SourceConfig struct {
	// MongoDB connection URI
	// Format: mongodb://username:password@host:port/database
	// For MongoDB Atlas: mongodb+srv://username:password@cluster.mongodb.net/database
	// For security, use environment variables or secret managers for credentials
	URI string `mapstructure:"uri" validate:"required"`

	// Database name to monitor
	Database string `mapstructure:"database" validate:"required"`

	// Collection name to monitor
	Collection string `mapstructure:"collection" validate:"required"`

	// TLS configuration for encrypted connections
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// Change stream options
	// FullDocument specifies what to return for update operations
	// Options: "default", "updateLookup", "whenAvailable", "required"
	FullDocument string `mapstructure:"fullDocument" default:"updateLookup"`

	// Pipeline to filter change stream events
	// Example: [{"$match": {"operationType": "insert"}}]
	Pipeline []bson.M `mapstructure:"pipeline"`

	// Resume token for starting the change stream from a specific point
	ResumeAfter bson.M `mapstructure:"resumeAfter"`

	// Start at operation time
	StartAtOperationTime *primitive.Timestamp `mapstructure:"startAtOperationTime"`

	// Connection timeout
	ConnectTimeout time.Duration `mapstructure:"connectTimeout" default:"10s" validate:"gt=0"`

	// Enable strict identifier validation (recommended: true)
	StrictValidation bool `mapstructure:"strictValidation" default:"true"`
}

type MongoSource struct {
	cfg    *SourceConfig
	slog   *slog.Logger
	c      chan *message.RunnerMessage
	client *mongo.Client
	stream *mongo.ChangeStream
	cancel context.CancelFunc
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

// validateIdentifier checks if a string is a valid MongoDB identifier
func validateIdentifier(name string, strict bool) error {
	if name == "" {
		return fmt.Errorf("identifier cannot be empty")
	}

	// MongoDB has a limit on database and collection names
	if len(name) > 64 {
		return fmt.Errorf("identifier exceeds maximum length of 64 characters")
	}

	// Check for invalid characters that could cause issues
	invalidChars := []string{"/", "\\", ".", " ", "\"", "$", "*", "<", ">", ":", "|", "?"}
	for _, char := range invalidChars {
		if len(name) > 0 && name[0:1] == char {
			return fmt.Errorf("identifier cannot start with '%s'", char)
		}
	}

	if strict {
		if !identifierRegex.MatchString(name) {
			return fmt.Errorf("invalid identifier: %s (must contain only alphanumeric, underscore, and hyphen)", name)
		}
	}

	return nil
}

// buildClientOptions creates MongoDB client options with TLS and timeout
func (s *MongoSource) buildClientOptions() (*options.ClientOptions, error) {
	// Resolve URI in case it contains secrets
	resolvedURI, err := secrets.Resolve(s.cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve URI: %w", err)
	}

	clientOpts := options.Client().ApplyURI(resolvedURI)

	// Apply TLS configuration
	tlsConf, err := tlsconfig.BuildClientConfigIfEnabled(s.cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	if tlsConf != nil {
		clientOpts.SetTLSConfig(tlsConf)
	}

	// Set connect timeout
	clientOpts.SetConnectTimeout(s.cfg.ConnectTimeout)

	return clientOpts, nil
}

// NewSource creates a MongoDB source from configuration
func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
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

	return &MongoSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "MongoDB Source"),
	}, nil
}

func (s *MongoSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	tlsEnabled := s.cfg.TLS != nil && s.cfg.TLS.Enabled

	s.slog.Info("starting MongoDB source",
		"database", s.cfg.Database,
		"collection", s.cfg.Collection,
		"tls", tlsEnabled,
		"strictValidation", s.cfg.StrictValidation,
		"fullDocument", s.cfg.FullDocument,
	)

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Build client options
	clientOpts, err := s.buildClientOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to build client options: %w", err)
	}

	// Create MongoDB client
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	s.client = client

	// Test connection
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		// Disconnect on error
		if disconnectErr := client.Disconnect(ctx); disconnectErr != nil {
			s.slog.Warn("failed to disconnect MongoDB client after ping error", "err", disconnectErr)
		}
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	// Get collection
	collection := client.Database(s.cfg.Database).Collection(s.cfg.Collection)

	// Build change stream options
	changeOpts := options.ChangeStream()
	if s.cfg.FullDocument != "" {
		changeOpts.SetFullDocument(options.FullDocument(s.cfg.FullDocument))
	}
	if s.cfg.ResumeAfter != nil {
		changeOpts.SetResumeAfter(s.cfg.ResumeAfter)
	}
	if s.cfg.StartAtOperationTime != nil {
		changeOpts.SetStartAtOperationTime(s.cfg.StartAtOperationTime)
	}

	// Create pipeline
	var pipeline mongo.Pipeline
	if len(s.cfg.Pipeline) > 0 {
		for _, stage := range s.cfg.Pipeline {
			// Convert bson.M to bson.D for pipeline
			var doc bson.D
			for k, v := range stage {
				doc = append(doc, bson.E{Key: k, Value: v})
			}
			pipeline = append(pipeline, doc)
		}
	}

	// Watch collection for changes
	stream, err := collection.Watch(ctx, pipeline, changeOpts)
	if err != nil {
		// Disconnect on error
		if disconnectErr := client.Disconnect(ctx); disconnectErr != nil {
			s.slog.Warn("failed to disconnect MongoDB client after watch error", "err", disconnectErr)
		}
		return nil, fmt.Errorf("failed to watch collection: %w", err)
	}
	s.stream = stream

	s.slog.Info("MongoDB change stream started", "database", s.cfg.Database, "collection", s.cfg.Collection)

	go s.watchLoop(ctx)

	return s.c, nil
}

func (s *MongoSource) watchLoop(ctx context.Context) {
	defer close(s.c)

	for s.stream.Next(ctx) {
		var event bson.M
		if err := s.stream.Decode(&event); err != nil {
			s.slog.Error("error decoding change stream event", "err", err)
			continue
		}

		s.slog.Debug("received change event", "operationType", event["operationType"])

		m := &MongoMessage{
			event: event,
		}
		s.c <- message.NewRunnerMessage(m)
	}

	if err := s.stream.Err(); err != nil {
		s.slog.Error("change stream error", "err", err)
	}
}

func (s *MongoSource) Close() error {
	if s.cancel != nil {
		s.cancel()
	}

	if s.stream != nil {
		if err := s.stream.Close(context.Background()); err != nil {
			s.slog.Error("error closing change stream", "err", err)
		}
	}

	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.client.Disconnect(ctx); err != nil {
			return fmt.Errorf("error disconnecting MongoDB client: %w", err)
		}
	}

	return nil
}
