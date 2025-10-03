package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandrolain/events-bridge/src/connectors"
	dbstore "github.com/sandrolain/events-bridge/src/connectors/pgsql/connect"
	"github.com/sandrolain/events-bridge/src/message"
)

type TargetConfig struct {
	ConnString         string                         `mapstructure:"connString" validate:"required"`
	Table              string                         `mapstructure:"table" validate:"required"`
	OtherColumn        string                         `mapstructure:"otherColumn"`
	OnConflict         dbstore.InsertRecordOnConflict `mapstructure:"onConflict" default:"DO NOTHING"`
	ConflictConstraint string                         `mapstructure:"conflictConstraint"`
	ConflictColumns    string                         `mapstructure:"conflictColumns"`
	BatchSize          int                            `mapstructure:"batchSize" default:"100" validate:"omitempty,min=1"`
}

func NewTargetConfig() any {
	return new(TargetConfig)
}

type PGSQLTarget struct {
	cfg  *TargetConfig
	slog *slog.Logger
	pool *pgxpool.Pool
}

// NewTarget creates a PGSQL target from config.
func NewTarget(anyCfg any) (connectors.Target, error) {
	cfg, ok := anyCfg.(*TargetConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PGSQLTarget{
		cfg:  cfg,
		slog: slog.Default().With("context", "PGSQL Target"),
		pool: pool,
	}, nil
}

func (t *PGSQLTarget) Consume(msg *message.RunnerMessage) error {
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

func (t *PGSQLTarget) Close() error {
	if t.pool != nil {
		t.pool.Close()
	}
	return nil
}
