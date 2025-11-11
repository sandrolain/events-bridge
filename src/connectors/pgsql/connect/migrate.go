package dbstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TableExistsArgs contains arguments for checking if a table exists
type TableExistsArgs struct {
	TableName string // Table name to check
}

// TableExists checks if a table exists in the database
func TableExists(ctx context.Context, db *pgxpool.Pool, args TableExistsArgs) (bool, error) {
	if args.TableName == "" {
		return false, ErrInvalidTableName
	}

	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_name = $1
		)
	`
	var exists bool
	err := db.QueryRow(ctx, query, args.TableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}
	return exists, nil
}

// CreateTableArgs contains arguments for creating a table
type CreateTableArgs struct {
	TableName string             // Table name to create
	Columns   []ColumnDefinition // Column definitions
}

// CreateTable creates a new table with the specified columns
// Returns nil if table creation succeeds
func CreateTable(ctx context.Context, db *pgxpool.Pool, args CreateTableArgs) error {
	if args.TableName == "" {
		return ErrInvalidTableName
	}
	if len(args.Columns) == 0 {
		return fmt.Errorf("at least one column definition is required")
	}

	// Build column definitions
	columnDefs := make([]string, len(args.Columns))
	for i, col := range args.Columns {
		if col.Name == "" {
			return fmt.Errorf("column name is required at index %d", i)
		}
		if col.Type == "" {
			return fmt.Errorf("column type is required for column %s", col.Name)
		}
		columnDefs[i] = fmt.Sprintf("%s %s", pgx.Identifier{col.Name}.Sanitize(), col.Type)
	}

	query := fmt.Sprintf(
		"CREATE TABLE %s (%s)",
		pgx.Identifier{args.TableName}.Sanitize(),
		strings.Join(columnDefs, ", "),
	)

	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}

// MigrateTableArgs contains arguments for table migration
type MigrateTableArgs struct {
	TableName string             // Table name to migrate
	Columns   []ColumnDefinition // Desired column definitions
}

// MigrateTable ensures the table structure matches the provided column definitions
// It adds missing columns and attempts to modify existing columns in a non-destructive way
// Returns nil if migration succeeds or if no changes are needed
func MigrateTable(ctx context.Context, db *pgxpool.Pool, args MigrateTableArgs) error {
	if args.TableName == "" {
		return ErrInvalidTableName
	}
	if len(args.Columns) == 0 {
		return nil // Nothing to migrate
	}

	// Get current table columns
	currentColumns, err := GetTableColumns(ctx, db, args.TableName)
	if err != nil {
		return fmt.Errorf("failed to get current table columns: %w", err)
	}

	// Create a map of existing columns for quick lookup
	existingCols := make(map[string]Column)
	for _, col := range currentColumns {
		existingCols[col.Name] = col
	}

	// Process each desired column
	for _, desiredCol := range args.Columns {
		if desiredCol.Name == "" {
			continue
		}

		existingCol, exists := existingCols[desiredCol.Name]
		if !exists {
			// Column doesn't exist - add it
			if err := addColumn(ctx, db, args.TableName, desiredCol); err != nil {
				return fmt.Errorf("failed to add column %s: %w", desiredCol.Name, err)
			}
		} else {
			// Column exists - check if type needs updating
			if err := alterColumnIfNeeded(ctx, db, args.TableName, existingCol, desiredCol); err != nil {
				return fmt.Errorf("failed to alter column %s: %w", desiredCol.Name, err)
			}
		}
	}

	// Clear cache for this table to force refresh
	cacheMutex.Lock()
	delete(columnCache, args.TableName)
	cacheMutex.Unlock()

	return nil
}

// addColumn adds a new column to an existing table
func addColumn(ctx context.Context, db *pgxpool.Pool, tableName string, col ColumnDefinition) error {
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s %s",
		pgx.Identifier{tableName}.Sanitize(),
		pgx.Identifier{col.Name}.Sanitize(),
		col.Type,
	)

	_, err := db.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to add column: %w", err)
	}
	return nil
}

// alterColumnIfNeeded modifies a column type if it differs from the desired type
// Only performs non-destructive changes (e.g., relaxing constraints, changing types if compatible)
// Skips columns with PRIMARY KEY, SERIAL, or other complex constraints
func alterColumnIfNeeded(ctx context.Context, db *pgxpool.Pool, tableName string, existing Column, desired ColumnDefinition) error {
	// Extract base type from existing column (remove metadata like |default=, |key=)
	existingBaseType := extractBaseType(existing.Type)
	desiredBaseType := extractBaseType(desired.Type)

	// Skip if the column has PRIMARY KEY constraint or is auto-increment (SERIAL)
	if strings.Contains(strings.ToUpper(existing.Type), "PRIMARY KEY") ||
		strings.Contains(strings.ToUpper(existing.Type), "NEXTVAL") ||
		strings.Contains(strings.ToUpper(desired.Type), "PRIMARY KEY") ||
		strings.Contains(strings.ToUpper(desired.Type), "SERIAL") {
		// Skip modification of primary keys and serial columns
		return nil
	}

	// Normalize types for comparison
	if normalizeType(existingBaseType) == normalizeType(desiredBaseType) {
		// Types match, no change needed
		return nil
	}

	// Attempt to alter column type
	// Use USING clause for safe conversion
	query := fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s",
		pgx.Identifier{tableName}.Sanitize(),
		pgx.Identifier{desired.Name}.Sanitize(),
		desiredBaseType,
		pgx.Identifier{desired.Name}.Sanitize(),
		desiredBaseType,
	)

	_, err := db.Exec(ctx, query)
	if err != nil {
		// If type change fails, log but don't fail migration
		// This is non-destructive - we keep the existing type
		return fmt.Errorf("failed to alter column type (non-critical): %w", err)
	}
	return nil
}

// extractBaseType extracts the base PostgreSQL type from a type string
// Removes metadata like |default=, |key=
func extractBaseType(typeStr string) string {
	parts := strings.Split(typeStr, "|")
	return strings.TrimSpace(parts[0])
}

// normalizeType normalizes PostgreSQL type names for comparison
func normalizeType(typeStr string) string {
	normalized := strings.ToLower(strings.TrimSpace(typeStr))

	// Normalize common type aliases
	switch normalized {
	case "int", "int4", "integer":
		return "integer"
	case "int8", "bigint":
		return "bigint"
	case "int2", "smallint":
		return "smallint"
	case "float4", "real":
		return "real"
	case "float8", "double precision":
		return "double precision"
	case "bool", "boolean":
		return "boolean"
	case "varchar", "character varying":
		return "character varying"
	case "char", "character":
		return "character"
	}

	return normalized
}

// EnsureTable creates the table if it doesn't exist, or migrates it if autoMigrate is enabled
type EnsureTableArgs struct {
	TableName   string             // Table name
	Columns     []ColumnDefinition // Column definitions
	AutoMigrate bool               // Enable automatic migration
}

// EnsureTable ensures a table exists and optionally migrates it to match the schema
// If the table doesn't exist, it creates it
// If autoMigrate is true and the table exists, it migrates the schema
func EnsureTable(ctx context.Context, db *pgxpool.Pool, args EnsureTableArgs) error {
	if args.TableName == "" {
		return ErrInvalidTableName
	}

	// Check if table exists
	exists, err := TableExists(ctx, db, TableExistsArgs{TableName: args.TableName})
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		// Table doesn't exist - create it
		if len(args.Columns) == 0 {
			return fmt.Errorf("cannot create table %s: no columns defined", args.TableName)
		}
		return CreateTable(ctx, db, CreateTableArgs{
			TableName: args.TableName,
			Columns:   args.Columns,
		})
	}

	// Table exists - migrate if enabled
	if args.AutoMigrate && len(args.Columns) > 0 {
		return MigrateTable(ctx, db, MigrateTableArgs{
			TableName: args.TableName,
			Columns:   args.Columns,
		})
	}

	return nil
}
