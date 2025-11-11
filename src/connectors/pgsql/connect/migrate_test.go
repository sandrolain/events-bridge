package dbstore

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cleanupTestTable drops a test table
func cleanupTestTable(t *testing.T, pool *pgxpool.Pool, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+tableName)
	require.NoError(t, err)
}

func TestTableExists(t *testing.T) {
	ctx := context.Background()
	tableName := "test_exists_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Table should not exist initially
	exists, err := TableExists(ctx, testDB, TableExistsArgs{TableName: tableName})
	require.NoError(t, err)
	assert.False(t, exists)

	// Create table
	_, err = testDB.Exec(ctx, "CREATE TABLE "+tableName+" (id SERIAL PRIMARY KEY)")
	require.NoError(t, err)

	// Table should now exist
	exists, err = TableExists(ctx, testDB, TableExistsArgs{TableName: tableName})
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestTableExists_EmptyName(t *testing.T) {
	ctx := context.Background()

	// Empty table name should return error
	exists, err := TableExists(ctx, testDB, TableExistsArgs{TableName: ""})
	require.Error(t, err)
	assert.Equal(t, ErrInvalidTableName, err)
	assert.False(t, exists)
}

func TestCreateTable(t *testing.T) {
	ctx := context.Background()
	tableName := "test_create_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Create table with columns
	columns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "name", Type: "VARCHAR(100) NOT NULL"},
		{Name: "data", Type: "JSONB"},
		{Name: "created_at", Type: "TIMESTAMP DEFAULT NOW()"},
	}

	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: tableName,
		Columns:   columns,
	})
	require.NoError(t, err)

	// Verify table exists
	exists, err := TableExists(ctx, testDB, TableExistsArgs{TableName: tableName})
	require.NoError(t, err)
	assert.True(t, exists)

	// Verify columns
	cols, err := GetTableColumns(ctx, testDB, tableName)
	require.NoError(t, err)
	assert.Len(t, cols, 4)

	// Check column names
	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}
	assert.Contains(t, colNames, "id")
	assert.Contains(t, colNames, "name")
	assert.Contains(t, colNames, "data")
	assert.Contains(t, colNames, "created_at")
}

func TestCreateTable_EmptyName(t *testing.T) {
	ctx := context.Background()

	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: "",
		Columns:   []ColumnDefinition{{Name: "id", Type: "SERIAL"}},
	})
	require.Error(t, err)
	assert.Equal(t, ErrInvalidTableName, err)
}

func TestCreateTable_NoColumns(t *testing.T) {
	ctx := context.Background()

	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: "test_table",
		Columns:   []ColumnDefinition{},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one column definition is required")
}

func TestMigrateTable_AddColumns(t *testing.T) {
	ctx := context.Background()
	tableName := "test_migrate_add_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Create initial table with 2 columns
	initialColumns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "name", Type: "VARCHAR(100)"},
	}
	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: tableName,
		Columns:   initialColumns,
	})
	require.NoError(t, err)

	// Migrate to add new columns
	desiredColumns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "name", Type: "VARCHAR(100)"},
		{Name: "email", Type: "VARCHAR(255)"},
		{Name: "age", Type: "INTEGER"},
	}
	err = MigrateTable(ctx, testDB, MigrateTableArgs{
		TableName: tableName,
		Columns:   desiredColumns,
	})
	require.NoError(t, err)

	// Verify new columns were added
	cols, err := GetTableColumns(ctx, testDB, tableName)
	require.NoError(t, err)
	assert.Len(t, cols, 4)

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}
	assert.Contains(t, colNames, "email")
	assert.Contains(t, colNames, "age")
}

func TestMigrateTable_NoChanges(t *testing.T) {
	ctx := context.Background()
	tableName := "test_migrate_nochange_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Create table
	columns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "name", Type: "VARCHAR(100)"},
	}
	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: tableName,
		Columns:   columns,
	})
	require.NoError(t, err)

	// Migrate with same schema
	err = MigrateTable(ctx, testDB, MigrateTableArgs{
		TableName: tableName,
		Columns:   columns,
	})
	require.NoError(t, err)

	// Verify no errors and table still has same columns
	cols, err := GetTableColumns(ctx, testDB, tableName)
	require.NoError(t, err)
	assert.Len(t, cols, 2)
}

func TestEnsureTable_CreateNew(t *testing.T) {
	ctx := context.Background()
	tableName := "test_ensure_new_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Ensure table (should create it)
	columns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "value", Type: "TEXT"},
	}
	err := EnsureTable(ctx, testDB, EnsureTableArgs{
		TableName:   tableName,
		Columns:     columns,
		AutoMigrate: false,
	})
	require.NoError(t, err)

	// Verify table was created
	exists, err := TableExists(ctx, testDB, TableExistsArgs{TableName: tableName})
	require.NoError(t, err)
	assert.True(t, exists)

	cols, err := GetTableColumns(ctx, testDB, tableName)
	require.NoError(t, err)
	assert.Len(t, cols, 2)
}

func TestEnsureTable_MigrateExisting(t *testing.T) {
	ctx := context.Background()
	tableName := "test_ensure_migrate_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Create initial table
	initialColumns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
	}
	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: tableName,
		Columns:   initialColumns,
	})
	require.NoError(t, err)

	// Ensure table with additional columns and AutoMigrate enabled
	desiredColumns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "status", Type: "VARCHAR(50)"},
	}
	err = EnsureTable(ctx, testDB, EnsureTableArgs{
		TableName:   tableName,
		Columns:     desiredColumns,
		AutoMigrate: true,
	})
	require.NoError(t, err)

	// Verify migration occurred
	cols, err := GetTableColumns(ctx, testDB, tableName)
	require.NoError(t, err)
	assert.Len(t, cols, 2)

	colNames := make([]string, len(cols))
	for i, col := range cols {
		colNames[i] = col.Name
	}
	assert.Contains(t, colNames, "status")
}

func TestEnsureTable_NoAutoMigrate(t *testing.T) {
	ctx := context.Background()
	tableName := "test_ensure_nomigrate_table"

	// Clean up
	cleanupTestTable(t, testDB, tableName)
	defer cleanupTestTable(t, testDB, tableName)

	// Create initial table
	initialColumns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
	}
	err := CreateTable(ctx, testDB, CreateTableArgs{
		TableName: tableName,
		Columns:   initialColumns,
	})
	require.NoError(t, err)

	// Ensure table with AutoMigrate disabled
	desiredColumns := []ColumnDefinition{
		{Name: "id", Type: "SERIAL PRIMARY KEY"},
		{Name: "extra", Type: "TEXT"},
	}
	err = EnsureTable(ctx, testDB, EnsureTableArgs{
		TableName:   tableName,
		Columns:     desiredColumns,
		AutoMigrate: false,
	})
	require.NoError(t, err)

	// Verify migration did NOT occur (still 1 column)
	cols, err := GetTableColumns(ctx, testDB, tableName)
	require.NoError(t, err)
	assert.Len(t, cols, 1)
}

func TestNormalizeType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"INT", "integer"},
		{"int4", "integer"},
		{"INTEGER", "integer"},
		{"BIGINT", "bigint"},
		{"int8", "bigint"},
		{"SMALLINT", "smallint"},
		{"int2", "smallint"},
		{"VARCHAR", "character varying"},
		{"BOOL", "boolean"},
		{"BOOLEAN", "boolean"},
		{"TEXT", "text"},
		{"JSONB", "jsonb"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractBaseType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"integer", "integer"},
		{"integer|default=nextval('seq')", "integer"},
		{"varchar|key=PRIMARY KEY", "varchar"},
		{"text|default=''|key=UNIQUE", "text"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := extractBaseType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
