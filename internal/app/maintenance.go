package app

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// MaintenanceConfig tunes the background reaper and retention purger.
type MaintenanceConfig struct {
	ReapInterval  time.Duration // default 5s; zero (with other zeros) triggers full defaults
	ReapBatch     int           // default 500
	PurgeInterval time.Duration // default 1m
	PurgeBatch    int           // default 1000
	// Retention is how long to keep finished jobs. Default 168h when the config is
	// zero-valued (ReapInterval == 0). Explicit Retention: 0 with a non-zero
	// ReapInterval means never purge.
	Retention time.Duration
}

// Maintenance runs reaping and purge loops until ctx is cancelled.
type Maintenance struct {
	repo    MaintenanceRepository
	clock   Clock
	cfg     MaintenanceConfig
	log     *slog.Logger
	metrics Metrics
	tracer  trace.Tracer
}

// NewMaintenance constructs a Maintenance runner with defaults applied.
//
// When ReapInterval is 0 (zero-value / unset config), all defaults are applied,
// including Retention = 168h. When the caller sets ReapInterval explicitly,
// Retention == 0 means never purge.
func NewMaintenance(repo MaintenanceRepository, clock Clock, cfg MaintenanceConfig, log *slog.Logger) *Maintenance {
	if log == nil {
		log = slog.Default()
	}
	fullDefaults := cfg.ReapInterval == 0
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
	if fullDefaults && cfg.Retention == 0 {
		cfg.Retention = 168 * time.Hour
	}
	return &Maintenance{repo: repo, clock: clock, cfg: cfg, log: log, metrics: NoopMetrics{}, tracer: tracerOrNoop(nil)}
}

// WithMetrics sets the metrics sink (nil → NoopMetrics).
func (m *Maintenance) WithMetrics(metrics Metrics) *Maintenance {
	m.metrics = metricsOrNoop(metrics)
	return m
}

// WithTracerProvider sets the tracer used for maintenance spans (nil → noop).
func (m *Maintenance) WithTracerProvider(tp trace.TracerProvider) *Maintenance {
	if tp == nil {
		m.tracer = tracerOrNoop(nil)
		return m
	}
	m.tracer = tp.Tracer(appTracerName)
	return m
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
	ctx, span := startSpan(ctx, m.tracer, "Maintenance.reap")
	var runErr error
	defer func() { endSpan(span, runErr) }()

	start := time.Now()
	affected := 0
	for ctx.Err() == nil {
		res, err := m.repo.ReapExpired(ctx, m.clock.Now(), m.cfg.ReapBatch)
		if err != nil {
			m.log.ErrorContext(ctx, "reap expired failed", "err", err)
			runErr = err
			break
		}
		n := res.Requeued + res.Dead
		affected += n
		if res.Requeued > 0 || res.Dead > 0 {
			m.log.InfoContext(ctx, "reaped expired leases", "requeued", res.Requeued, "dead", res.Dead)
		}
		if n < m.cfg.ReapBatch {
			break
		}
	}
	m.metrics.MaintenanceRun("reap", affected, time.Since(start), runErr)
}

func (m *Maintenance) purgeOnce(ctx context.Context) {
	if m.cfg.Retention == 0 {
		return
	}
	ctx, span := startSpan(ctx, m.tracer, "Maintenance.purge")
	var runErr error
	defer func() { endSpan(span, runErr) }()

	start := time.Now()
	affected := 0
	before := m.clock.Now().Add(-m.cfg.Retention)
	for ctx.Err() == nil {
		n, err := m.repo.PurgeFinished(ctx, before, m.cfg.PurgeBatch)
		if err != nil {
			m.log.ErrorContext(ctx, "purge finished failed", "err", err)
			runErr = err
			break
		}
		affected += n
		if n > 0 {
			m.log.InfoContext(ctx, "purged finished jobs", "count", n, "before", before)
		}
		if n < m.cfg.PurgeBatch {
			break
		}
	}
	m.metrics.MaintenanceRun("purge", affected, time.Since(start), runErr)
}
