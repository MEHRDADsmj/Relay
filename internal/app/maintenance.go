package app

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

// MaintenanceConfig tunes the background reaper and retention purger.
type MaintenanceConfig struct {
	ReapInterval  time.Duration // default 5s
	ReapBatch     int           // default 500
	PurgeInterval time.Duration // default 1m
	PurgeBatch    int           // default 1000
	Retention     time.Duration // default 168h; 0 = never purge
}

// Maintenance runs reaping and purge loops until ctx is cancelled.
type Maintenance struct {
	repo  MaintenanceRepository
	clock Clock
	cfg   MaintenanceConfig
	log   *slog.Logger
}

// NewMaintenance constructs a Maintenance runner with defaults applied.
func NewMaintenance(repo MaintenanceRepository, clock Clock, cfg MaintenanceConfig, log *slog.Logger) *Maintenance {
	if log == nil {
		log = slog.Default()
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
	}
	if cfg.ReapBatch <= 0 {
		cfg.ReapBatch = 500
	}
	if cfg.PurgeInterval <= 0 {
		cfg.PurgeInterval = time.Minute
	}
	if cfg.PurgeBatch <= 0 {
		cfg.PurgeBatch = 1000
	}
	if cfg.Retention < 0 {
		cfg.Retention = 168 * time.Hour
	}
	return &Maintenance{repo: repo, clock: clock, cfg: cfg, log: log}
}

// Run starts reaper and purger loops. It returns only when ctx is done.
// Transient errors are logged and retried on the next tick.
func (m *Maintenance) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.reapLoop(gctx) })
	g.Go(func() error { return m.purgeLoop(gctx) })
	_ = g.Wait()
	return ctx.Err()
}

func (m *Maintenance) reapLoop(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.ReapInterval)
	defer ticker.Stop()
	for {
		m.reapOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Maintenance) purgeLoop(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.PurgeInterval)
	defer ticker.Stop()
	for {
		m.purgeOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *Maintenance) reapOnce(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		res, err := m.repo.ReapExpired(ctx, m.clock.Now(), m.cfg.ReapBatch)
		if err != nil {
			m.log.Error("reap expired failed", "err", err)
			return
		}
		if res.Requeued > 0 || res.Dead > 0 {
			m.log.Info("reaped expired leases", "requeued", res.Requeued, "dead", res.Dead)
		}
		if res.Requeued+res.Dead < m.cfg.ReapBatch {
			return
		}
	}
}

func (m *Maintenance) purgeOnce(ctx context.Context) {
	if m.cfg.Retention == 0 {
		return
	}
	before := m.clock.Now().Add(-m.cfg.Retention)
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := m.repo.PurgeFinished(ctx, before, m.cfg.PurgeBatch)
		if err != nil {
			m.log.Error("purge finished failed", "err", err)
			return
		}
		if n > 0 {
			m.log.Info("purged finished jobs", "count", n, "before", before)
		}
		if n < m.cfg.PurgeBatch {
			return
		}
	}
}
