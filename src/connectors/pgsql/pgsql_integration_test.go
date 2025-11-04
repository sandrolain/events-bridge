//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	testTable       = "test_events"
	testDBName      = "testdb"
	testDBUser      = "testuser"
	testDBPassword  = "testpass"
	testChannelName = "test_events_changes"
)

var (
	pgContainer testcontainers.Container
	connString  string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup PostgreSQL container
	pgC, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase(testDBName),
		postgres.WithUsername(testDBUser),
		postgres.WithPassword(testDBPassword),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to start PostgreSQL container: %v", err))
	}
	pgContainer = pgC

	// Get connection string
	host, err := pgC.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get PostgreSQL host: %v", err))
	}
	port, err := pgC.MappedPort(ctx, "5432/tcp")
	if err != nil {
		panic(fmt.Sprintf("failed to get PostgreSQL port: %v", err))
	}
	connString = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		testDBUser, testDBPassword, host, port.Port(), testDBName)

	// Wait a bit for PostgreSQL to be fully ready
	time.Sleep(2 * time.Second)

	// Setup test table
	if err := setupTestTable(ctx); err != nil {
		panic(fmt.Sprintf("failed to setup test table: %v", err))
	} // Run tests
	code := m.Run()

	// Cleanup
	if err := pgContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate PostgreSQL container: %v\n", err)
	}

	os.Exit(code)
}

func setupTestTable(ctx context.Context) error {
	var conn *pgx.Conn
	var err error

	// Retry connection up to 5 times
	for i := 0; i < 5; i++ {
		conn, err = pgx.Connect(ctx, connString)
		if err == nil {
			break
		}
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL after retries: %w", err)
	}
	defer conn.Close(ctx)

	// Create test table
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			data JSONB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`, testTable)

	_, err = conn.Exec(ctx, createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}

	return nil
}

func TestPostgreSQLSourceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source configuration
	sourceCfg := &SourceConfig{
		ConnString: connString,
		Table:      testTable,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait a bit for the source to setup triggers and start listening
	time.Sleep(2 * time.Second)

	// Insert test data to trigger notification
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	testName := "integration test record"
	testData := `{"test": "value", "number": 42}`

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (name, data) 
		VALUES ($1, $2) 
		RETURNING id
	`, testTable)

	var insertedID int
	err = testConn.QueryRow(ctx, insertQuery, testName, testData).Scan(&insertedID)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		// Verify the notification contains the expected data
		dataStr := string(data)
		assert.Contains(t, dataStr, testName)
		assert.Contains(t, dataStr, "INSERT")
		assert.Contains(t, dataStr, testTable)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, testChannelName, metadata["channel"])

		// Acknowledge the message
		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for PostgreSQL notification")
	}
}

func TestPostgreSQLSourceUpdateIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source configuration
	sourceCfg := &SourceConfig{
		ConnString: connString,
		Table:      testTable,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait for setup
	time.Sleep(2 * time.Second)

	// Insert and then update test data
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	testName := "update test record"
	testData := `{"test": "original", "status": "pending"}`

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (name, data) 
		VALUES ($1, $2) 
		RETURNING id
	`, testTable)

	var insertedID int
	err = testConn.QueryRow(ctx, insertQuery, testName, testData).Scan(&insertedID)
	require.NoError(t, err)

	// Consume INSERT notification
	select {
	case <-msgChan:
		// Expected INSERT notification
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for INSERT notification")
	}

	// Now update the record
	updatedData := `{"test": "updated", "status": "completed"}`
	updateQuery := fmt.Sprintf(`
		UPDATE %s 
		SET data = $1 
		WHERE id = $2
	`, testTable)

	_, err = testConn.Exec(ctx, updateQuery, updatedData, insertedID)
	require.NoError(t, err)

	// Wait for UPDATE notification
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		dataStr := string(data)
		assert.Contains(t, dataStr, "UPDATE")
		assert.Contains(t, dataStr, testTable)
		assert.Contains(t, dataStr, "updated")
		assert.Contains(t, dataStr, "completed")

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, testChannelName, metadata["channel"])

		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for UPDATE notification")
	}
}

func TestPostgreSQLSourceDeleteIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source configuration
	sourceCfg := &SourceConfig{
		ConnString: connString,
		Table:      testTable,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait for setup
	time.Sleep(2 * time.Second)

	// Insert test data to delete
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	testName := "delete test record"
	testData := `{"test": "to_be_deleted", "status": "temporary"}`

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (name, data) 
		VALUES ($1, $2) 
		RETURNING id
	`, testTable)

	var insertedID int
	err = testConn.QueryRow(ctx, insertQuery, testName, testData).Scan(&insertedID)
	require.NoError(t, err)

	// Consume INSERT notification
	select {
	case <-msgChan:
		// Expected INSERT notification
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for INSERT notification")
	}

	// Now delete the record
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s 
		WHERE id = $1
	`, testTable)

	_, err = testConn.Exec(ctx, deleteQuery, insertedID)
	require.NoError(t, err)

	// Wait for DELETE notification
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		dataStr := string(data)
		assert.Contains(t, dataStr, "DELETE")
		assert.Contains(t, dataStr, testTable)
		assert.Contains(t, dataStr, testName)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, testChannelName, metadata["channel"])

		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for DELETE notification")
	}
}

func TestPostgreSQLSourceMultipleOperationsIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Setup source configuration
	sourceCfg := &SourceConfig{
		ConnString: connString,
		Table:      testTable,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(20)
	require.NoError(t, err)

	// Wait for setup
	time.Sleep(2 * time.Second)

	// Perform multiple operations
	testConn, err := pgx.Connect(ctx, connString)
	require.NoError(t, err)
	defer testConn.Close(ctx)

	numOperations := 3
	expectedOperations := []string{"INSERT", "INSERT", "INSERT"}

	// Insert multiple records
	for i := 0; i < numOperations; i++ {
		testName := fmt.Sprintf("multi test record %d", i)
		testData := fmt.Sprintf(`{"test": "multi", "index": %d}`, i)

		insertQuery := fmt.Sprintf(`
			INSERT INTO %s (name, data) 
			VALUES ($1, $2)
		`, testTable)

		_, err = testConn.Exec(ctx, insertQuery, testName, testData)
		require.NoError(t, err)
	}

	// Collect all notifications
	receivedCount := 0
	receivedOps := make([]string, 0, numOperations)

	for receivedCount < numOperations {
		select {
		case receivedMsg := <-msgChan:
			data, err := receivedMsg.GetSourceData()
			require.NoError(t, err)

			dataStr := string(data)

			// Determine operation type
			var operation string
			if assert.Contains(t, dataStr, "INSERT") {
				operation = "INSERT"
			} else if assert.Contains(t, dataStr, "UPDATE") {
				operation = "UPDATE"
			} else if assert.Contains(t, dataStr, "DELETE") {
				operation = "DELETE"
			}

			receivedOps = append(receivedOps, operation)
			assert.Contains(t, dataStr, testTable)

			metadata, err := receivedMsg.GetSourceMetadata()
			require.NoError(t, err)
			assert.Equal(t, testChannelName, metadata["channel"])

			err = receivedMsg.Ack()
			assert.NoError(t, err)

			receivedCount++

		case <-ctx.Done():
			t.Fatalf("timeout waiting for notifications, received %d out of %d", receivedCount, numOperations)
		}
	}

	assert.Equal(t, numOperations, receivedCount)
	assert.Equal(t, expectedOperations, receivedOps)
}
