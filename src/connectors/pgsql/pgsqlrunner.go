package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandrolain/events-bridge/src/common/tlsconfig"
	"github.com/sandrolain/events-bridge/src/connectors"
	dbstore "github.com/sandrolain/events-bridge/src/connectors/pgsql/connect"
	"github.com/sandrolain/events-bridge/src/message"
)

// RunnerConfig defines the configuration for PostgreSQL runner connector
type RunnerConfig struct {
	// Database connection string
	// Format: postgres://user:password@host:port/database?sslmode=disable
	// For security, use environment variables or secret managers for credentials
	ConnString string `mapstructure:"connString" validate:"required"`

	// Target table name for inserting records
	// Must be a valid PostgreSQL identifier (alphanumeric + underscore)
	Table string `mapstructure:"table" validate:"required"`

	// Additional column name for metadata or other data
	OtherColumn string `mapstructure:"otherColumn"`

	// Conflict resolution strategy: DO NOTHING, DO UPDATE
	OnConflict dbstore.InsertRecordOnConflict `mapstructure:"onConflict" default:"DO NOTHING"`

	// Constraint name for conflict resolution (e.g., unique constraint)
	ConflictConstraint string `mapstructure:"conflictConstraint"`

	// Columns to check for conflicts (comma-separated)
	ConflictColumns string `mapstructure:"conflictColumns"`

	// Number of records to batch before inserting
	BatchSize int `mapstructure:"batchSize" default:"100" validate:"omitempty,min=1"`

	// TLS configuration for encrypted connections
	TLS *tlsconfig.Config `mapstructure:"tls"`

	// Maximum number of connections in the pool
	MaxConns int32 `mapstructure:"maxConns" default:"10" validate:"omitempty,min=1,max=100"`

	// Minimum number of connections in the pool
	MinConns int32 `mapstructure:"minConns" default:"2" validate:"omitempty,min=0,max=10"`

	// Enable strict identifier validation (recommended: true)
	StrictValidation bool `mapstructure:"strictValidation" default:"true"`
}

func NewRunnerConfig() any {
	return new(RunnerConfig)
}

type PGSQLRunner struct {
	cfg  *RunnerConfig
	slog *slog.Logger
	pool *pgxpool.Pool
}

// buildPoolConfig creates pgxpool configuration with TLS and connection limits
func (t *PGSQLRunner) buildPoolConfig() (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(t.cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Apply TLS configuration
	tlsConf, err := tlsconfig.BuildClientConfigIfEnabled(t.cfg.TLS)
	if err != nil {
		return nil, err
	}

	if tlsConf != nil {
		config.ConnConfig.TLSConfig = tlsConf
	}

	// Apply connection pool limits
	config.MaxConns = t.cfg.MaxConns
	config.MinConns = t.cfg.MinConns

	return config, nil
}

// NewRunner creates a PGSQL target from config.
func NewRunner(anyCfg any) (connectors.Runner, error) {
	cfg, ok := anyCfg.(*RunnerConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	// Validate table name to prevent SQL injection
	if err := validateIdentifier(cfg.Table, cfg.StrictValidation); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	// Validate other column if provided
	if cfg.OtherColumn != "" {
		if err := validateIdentifier(cfg.OtherColumn, cfg.StrictValidation); err != nil {
			return nil, fmt.Errorf("invalid other column name: %w", err)
		}
	}

	target := &PGSQLRunner{
		cfg:  cfg,
		slog: slog.Default().With("context", "PGSQL Target"),
	}

	// Build pool configuration with TLS
	poolConfig, err := target.buildPoolConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build pool config: %w", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	target.pool = pool

	tlsEnabled := cfg.TLS != nil && cfg.TLS.Enabled
	target.slog.Info("PGSQL target connected",
		"table", cfg.Table,
		"tls", tlsEnabled,
		"strictValidation", cfg.StrictValidation,
		"batchSize", cfg.BatchSize,
	)

	return target, nil
}

func (t *PGSQLRunner) Process(msg *message.RunnerMessage) error {
	ctx := context.Background()

	data, err := msg.GetData()
	if err != nil {
		return fmt.Errorf("failed to get message data: %w", err)
	}

	// Parse data as JSON to create a record
	var record dbstore.Record
	if err := json.Unmarshal(data, &record); err != nil {
		return fmt.Errorf("failed to unmarshal message data: %w", err)
	}

	// Add metadata as additional fields if needed
	metadata, err := msg.GetMetadata()
	if err != nil {
		return fmt.Errorf("failed to get message metadata: %w", err)
	}

	// Optionally include metadata in the record
	for k, v := range metadata {
		if _, exists := record[k]; !exists {
			record[k] = v
		}
	}

	t.slog.Debug("inserting record", "table", t.cfg.Table, "record", record)

	// Use InsertRecord to insert the record
	args := dbstore.InsertRecordArgs{
		TableName:          t.cfg.Table,
		OtherColumn:        t.cfg.OtherColumn,
		BatchRecords:       []dbstore.Record{record},
		OnConflict:         t.cfg.OnConflict,
		ConflictConstraint: t.cfg.ConflictConstraint,
		ConflictColumns:    t.cfg.ConflictColumns,
		BatchSize:          t.cfg.BatchSize,
	}

	if err := dbstore.InsertRecord(ctx, t.pool, args); err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	t.slog.Debug("record inserted successfully", "table", t.cfg.Table)
	return nil
}

func (t *PGSQLRunner) Close() error {
	if t.pool != nil {
		t.pool.Close()
	}
	return nil
}
