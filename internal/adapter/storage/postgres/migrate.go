package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"

	_ "github.com/jackc/pgx/v5/stdlib" // register "pgx" database/sql driver
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Migrate applies all embedded goose migrations to the database at databaseURL.
func Migrate(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("postgres migrate: open: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("postgres migrate: ping: %w", err)
	}

	return MigrateDB(ctx, db)
}

// MigrateDB applies all embedded goose migrations using an existing *sql.DB.
func MigrateDB(ctx context.Context, db *sql.DB) error {
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("postgres migrate: fs: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectPostgres, db, sub)
	if err != nil {
		return fmt.Errorf("postgres migrate: provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("postgres migrate: up: %w", err)
	}
	return nil
}
