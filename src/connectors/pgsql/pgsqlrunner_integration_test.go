//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTargetTable = "test_runner_table"
)

func TestPostgreSQLTargetIntegration(t *testing.T) {
	ctx := context.Background()

	// Setup runner table
	if err := setupTargetTable(ctx); err != nil {
		t.Fatalf("failed to setup runner table: %v", err)
	}

	// Create runner configuration
	runnerCfg := &RunnerConfig{
		ConnString:  connString,
		Table:       testTargetTable,
		OtherColumn: "extra_data",
		OnConflict:  "DO NOTHING",
		BatchSize:   10,
	}

	// Create runner
	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Create test message
	testData := map[string]interface{}{
		"name":         "test record",
		"value":        42,
		"description":  "integration test",
		"custom_field": "this should go to extra_data",
	}

	dataBytes, err := json.Marshal(testData)
	require.NoError(t, err)

	// Create a runner message with test data
	sourceMsg := &testSourceMessage{
		id:       []byte("test-id-1"),
		data:     dataBytes,
		metadata: map[string]string{"source": "test"},
	}
	runnerMsg := message.NewRunnerMessage(sourceMsg)

	// Consume the message
	err = runner.Process(runnerMsg)
	require.NoError(t, err)

	// Verify the record was inserted
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	var name, description string
	var value int
	var extraData map[string]interface{}

	query := fmt.Sprintf(`
		SELECT name, value, description, extra_data
		FROM %s
		WHERE name = $1
	`, testTargetTable)

	err = testConn.QueryRow(ctx, query, "test record").Scan(&name, &value, &description, &extraData)
	require.NoError(t, err)

	assert.Equal(t, "test record", name)
	assert.Equal(t, 42, value)
	assert.Equal(t, "integration test", description)
	assert.NotNil(t, extraData)
	assert.Equal(t, "this should go to extra_data", extraData["custom_field"])
}

func TestPostgreSQLTargetBatchIntegration(t *testing.T) {
	ctx := context.Background()

	// Setup runner table
	if err := setupTargetTable(ctx); err != nil {
		t.Fatalf("failed to setup runner table: %v", err)
	}

	// Create runner configuration
	runnerCfg := &RunnerConfig{
		ConnString:  connString,
		Table:       testTargetTable,
		OtherColumn: "extra_data",
		OnConflict:  "DO NOTHING",
		BatchSize:   10,
	}

	// Create runner
	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Insert multiple messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		testData := map[string]interface{}{
			"name":        fmt.Sprintf("batch record %d", i),
			"value":       i * 10,
			"description": fmt.Sprintf("batch test %d", i),
		}

		dataBytes, err := json.Marshal(testData)
		require.NoError(t, err)

		sourceMsg := &testSourceMessage{
			id:       []byte(fmt.Sprintf("test-id-%d", i)),
			data:     dataBytes,
			metadata: map[string]string{"source": "batch-test"},
		}
		runnerMsg := message.NewRunnerMessage(sourceMsg)

		err = runner.Process(runnerMsg)
		require.NoError(t, err)
	}

	// Verify all records were inserted
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	query := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM %s 
		WHERE name LIKE 'batch record %%'
	`, testTargetTable)

	var count int
	err = testConn.QueryRow(ctx, query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, numMessages, count)
}

func TestPostgreSQLTargetOnConflictUpdate(t *testing.T) {
	ctx := context.Background()

	// Setup runner table with unique constraint
	if err := setupTargetTableWithConstraint(ctx); err != nil {
		t.Fatalf("failed to setup runner table: %v", err)
	}

	// Create runner configuration with DO UPDATE on conflict
	runnerCfg := &RunnerConfig{
		ConnString:      connString,
		Table:           testTargetTable,
		OtherColumn:     "extra_data",
		OnConflict:      "DO UPDATE",
		ConflictColumns: "name",
		BatchSize:       10,
	}

	// Create runner
	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Insert first record
	testData := map[string]interface{}{
		"name":        "unique record",
		"value":       100,
		"description": "original",
	}

	dataBytes, err := json.Marshal(testData)
	require.NoError(t, err)

	sourceMsg := &testSourceMessage{
		id:       []byte("test-id-unique-1"),
		data:     dataBytes,
		metadata: map[string]string{},
	}
	runnerMsg := message.NewRunnerMessage(sourceMsg)

	err = runner.Process(runnerMsg)
	require.NoError(t, err)

	// Try to insert same record with updated values
	testData["value"] = 200
	testData["description"] = "updated"

	dataBytes, err = json.Marshal(testData)
	require.NoError(t, err)

	sourceMsg = &testSourceMessage{
		id:       []byte("test-id-unique-2"),
		data:     dataBytes,
		metadata: map[string]string{},
	}
	runnerMsg = message.NewRunnerMessage(sourceMsg)

	err = runner.Process(runnerMsg)
	require.NoError(t, err)

	// Verify the record was updated
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	var value int
	var description string

	query := fmt.Sprintf(`
		SELECT value, description
		FROM %s
		WHERE name = $1
	`, testTargetTable)

	err = testConn.QueryRow(ctx, query, "unique record").Scan(&value, &description)
	require.NoError(t, err)

	assert.Equal(t, 200, value)
	assert.Equal(t, "updated", description)

	// Verify only one record exists
	var count int
	countQuery := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM %s 
		WHERE name = $1
	`, testTargetTable)

	err = testConn.QueryRow(ctx, countQuery, "unique record").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// Helper functions

func setupTargetTable(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	// Drop table if exists
	dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, testTargetTable)
	_, err = conn.Exec(ctx, dropQuery)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Create test table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			value INTEGER,
			description TEXT,
			extra_data JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, testTargetTable)

	_, err = conn.Exec(ctx, createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func setupTargetTableWithConstraint(ctx context.Context) error {
	if err := setupTargetTable(ctx); err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	// Add unique constraint on name column
	constraintQuery := fmt.Sprintf(`
		ALTER TABLE %s 
		ADD CONSTRAINT unique_name UNIQUE (name)
	`, testTargetTable)

	_, err = conn.Exec(ctx, constraintQuery)
	if err != nil {
		return fmt.Errorf("failed to add constraint: %w", err)
	}

	return nil
}

// testSourceMessage implements message.SourceMessage for testing
type testSourceMessage struct {
	id       []byte
	data     []byte
	metadata map[string]string
}

func (m *testSourceMessage) GetID() []byte {
	return m.id
}

func (m *testSourceMessage) GetMetadata() (map[string]string, error) {
	return m.metadata, nil
}

func (m *testSourceMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *testSourceMessage) Ack(*message.ReplyData) error {
	return nil
}

func (m *testSourceMessage) Nak() error {
	return nil
}

