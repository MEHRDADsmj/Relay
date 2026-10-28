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
	return withProvider(ctx, databaseURL, func(ctx context.Context, p *goose.Provider) error {
		if _, err := p.Up(ctx); err != nil {
			return fmt.Errorf("postgres migrate: up: %w", err)
		}
		return nil
	})
}

// MigrateDown rolls back the most recently applied migration.
func MigrateDown(ctx context.Context, databaseURL string) error {
	return withProvider(ctx, databaseURL, func(ctx context.Context, p *goose.Provider) error {
		if _, err := p.Down(ctx); err != nil {
			return fmt.Errorf("postgres migrate: down: %w", err)
		}
		return nil
	})
}

// MigrateStatus returns the status of each embedded migration.
func MigrateStatus(ctx context.Context, databaseURL string) ([]*goose.MigrationStatus, error) {
	var statuses []*goose.MigrationStatus
	err := withProvider(ctx, databaseURL, func(ctx context.Context, p *goose.Provider) error {
		var err error
		statuses, err = p.Status(ctx)
		if err != nil {
			return fmt.Errorf("postgres migrate: status: %w", err)
		}
		return nil
	})
	return statuses, err
}

// MigrateDB applies all embedded goose migrations using an existing *sql.DB.
func MigrateDB(ctx context.Context, db *sql.DB) error {
	provider, err := newProvider(db)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("postgres migrate: up: %w", err)
	}
	return nil
}

func withProvider(ctx context.Context, databaseURL string, fn func(context.Context, *goose.Provider) error) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("postgres migrate: open: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("postgres migrate: ping: %w", err)
	}

	provider, err := newProvider(db)
	if err != nil {
		return err
	}
	return fn(ctx, provider)
}

func newProvider(db *sql.DB) (*goose.Provider, error) {
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("postgres migrate: fs: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, sub)
	if err != nil {
		return nil, fmt.Errorf("postgres migrate: provider: %w", err)
	}
	return provider, nil
}
