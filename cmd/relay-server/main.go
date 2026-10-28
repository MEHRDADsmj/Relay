// Command relay-server is the composition root for the Relay job queue.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/MEHRDADsmj/relay/internal/adapter/admin"
	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	adaptergrpc "github.com/MEHRDADsmj/relay/internal/adapter/grpc"
	adapterhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/metrics/prom"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres"
	redisstore "github.com/MEHRDADsmj/relay/internal/adapter/storage/redis"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
	"github.com/MEHRDADsmj/relay/internal/platform/config"
	"github.com/MEHRDADsmj/relay/internal/platform/logging"
	"github.com/MEHRDADsmj/relay/internal/platform/telemetry"
)

// Set via -ldflags at build time (Phase 4 GoReleaser).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "relay-server: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if wantsVersion(args) {
		fmt.Printf("relay-server %s (commit=%s date=%s go=%s)\n",
			version, commit, date, runtime.Version())
		return nil
	}
	cmd, rest := parseCommand(args)
	switch cmd {
	case "serve", "":
		return runServe()
	case "migrate":
		return runMigrate(rest)
	case "healthcheck":
		return runHealthcheck()
	case "apikey":
		return runAPIKey(rest)
	default:
		return fmt.Errorf("unknown command %q (want serve|migrate|healthcheck|apikey|--version)", cmd)
	}
}

func wantsVersion(args []string) bool {
	for _, a := range args {
		switch a {
		case "--version", "-version", "version":
			return true
		}
	}
	return false
}

func parseCommand(args []string) (cmd string, rest []string) {
	if len(args) == 0 {
		return "serve", nil
	}
	if strings.HasPrefix(args[0], "-") {
		return "serve", args
	}
	return args[0], args[1:]
}

func runServe() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	telShutdown, err := telemetry.Setup(ctx, telemetry.Config{
		Endpoint:       cfg.OTLPEndpoint,
		SampleRatio:    cfg.TraceSampleRatio,
		ServiceVersion: version,
		InstanceID:     cfg.InstanceID,
		Environment:    cfg.Environment,
	})
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	defer func() {
		shCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := telShutdown(shCtx); err != nil {
			log.ErrorContext(shCtx, "telemetry shutdown", "err", err)
		}
	}()

	repo, cleanup, err := openRepository(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer cleanup()

	if cfg.MigrateOnStart {
		if err := migrateOnStart(ctx, cfg, log); err != nil {
			return err
		}
	}

	// Apply configured long-poll cap before any LeaseWait calls.
	domain.MaxLeaseWait = cfg.LeaseMaxWait

	registry := prometheus.NewRegistry()
	if err := prom.RegisterRuntime(registry); err != nil {
		return fmt.Errorf("register runtime metrics: %w", err)
	}
	if err := prom.RegisterBuildInfo(registry, version, commit, runtime.Version(), cfg.Storage); err != nil {
		return fmt.Errorf("register build info: %w", err)
	}
	bizMetrics, err := prom.New(prom.Options{
		MaxSeries:  cfg.MetricsMaxSeries,
		Registerer: registry,
	})
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	httpMetrics, err := prom.NewHTTPMetrics(registry)
	if err != nil {
		return fmt.Errorf("http metrics: %w", err)
	}
	grpcMetrics := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(),
	)
	if err := registry.Register(grpcMetrics); err != nil {
		return fmt.Errorf("grpc metrics: %w", err)
	}
	if err := registerStoragePoolMetrics(registry, repo); err != nil {
		return err
	}

	clock := app.SystemClock{}
	notifier := mustNotifier(repo)
	otelTP := otel.GetTracerProvider()
	svc := app.NewJobService(
		repo,
		clock,
		app.UUIDv7Generator{},
		newRetryPolicy(cfg),
		log,
		notifier,
	).WithMetrics(bizMetrics).WithTracerProvider(otelTP)
	queues := app.NewQueueService(mustQueueRepo(repo), repo, clock)
	queueStats := prom.NewQueueStatsCollector(queues, prom.QueueStatsOptions{})
	if err := registry.Register(queueStats); err != nil {
		return fmt.Errorf("queue stats collector: %w", err)
	}
	if err := registry.Register(queueStats.CollectErrors); err != nil {
		return fmt.Errorf("queue stats errors: %w", err)
	}
	apiKeyRepo := mustAPIKeyRepo(repo)
	scheduleRepo := mustScheduleRepo(repo)
	auth := app.NewAuthService(apiKeyRepo, clock, app.AuthServiceConfig{
		BootstrapKey: cfg.APIKey,
		CacheTTL:     cfg.AuthCacheTTL,
	}, log)
	warnAuthConfig(ctx, log, cfg.APIKey, apiKeyRepo)
	cronParser := adaptercron.NewParser()
	schedules := app.NewScheduleService(scheduleRepo, svc, cronParser, clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(apiKeyRepo, clock)
	handler := adapterhttp.NewRouter(svc, queues, schedules, apiKeys, adapterhttp.Options{
		APIKey:  cfg.APIKey,
		Auth:    auth,
		Logger:  log,
		Metrics: httpMetrics.Middleware,
		Tracing: true,
	})

	maint := app.NewMaintenance(mustMaintenanceRepo(repo), clock, app.MaintenanceConfig{
		ReapInterval:  cfg.ReapInterval,
		ReapBatch:     cfg.ReapBatch,
		PurgeInterval: cfg.PurgeInterval,
		PurgeBatch:    cfg.PurgeBatch,
		Retention:     cfg.Retention,
	}, log).WithMetrics(bizMetrics).WithTracerProvider(otelTP)

	elector, err := newLeaderElector(repo, cfg)
	if err != nil {
		return err
	}
	scheduler := app.NewScheduler(scheduleRepo, svc, elector, cronParser, clock, app.SchedulerConfig{
		Tick: cfg.SchedulerTick,
	}, log).WithMetrics(bizMetrics).WithTracerProvider(otelTP)

	grpcSrv := adaptergrpc.NewServer(adaptergrpc.Services{
		Jobs:      svc,
		Queues:    queues,
		Schedules: schedules,
		APIKeys:   apiKeys,
	}, adaptergrpc.Options{
		Auth:          auth,
		Logger:        log,
		Reflection:    cfg.GRPCReflection,
		Ready:         repo.Ping,
		ServerMetrics: grpcMetrics,
		Tracing:       true,
	})
	grpcLis, err := adaptergrpc.Listen(cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	adminSrv := admin.New(admin.Options{
		Addr:     cfg.AdminAddr,
		Gatherer: registry,
		Ready:    repo.Ping,
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// WriteTimeout must exceed max lease wait_seconds; see phase-2 §5.
		WriteTimeout: cfg.LeaseMaxWait + 15*time.Second,
		IdleTimeout:  60 * time.Second,
		// Cancel in-flight request contexts (incl. LeaseWait long-polls) on SIGTERM so
		// Shutdown does not wait out LeaseMaxWait. Handler maps context.Canceled → 200 empty.
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}
	if cfg.ShutdownTimeout < cfg.LeaseMaxWait {
		// With BaseContext, long-polls cancel immediately; still warn if drain budget is
		// shorter than the configured max wait (operators may expect the old behaviour).
		log.WarnContext(ctx, "RELAY_SHUTDOWN_TIMEOUT is less than RELAY_LEASE_MAX_WAIT; BaseContext cancels long-polls on shutdown",
			"shutdown_timeout", cfg.ShutdownTimeout,
			"lease_max_wait", cfg.LeaseMaxWait,
		)
	}

	// Scheduler resigns before HTTP/gRPC drain; maintenance stops after drain (phase-3 §8).
	schedCtx, schedCancel := context.WithCancel(context.Background())
	defer schedCancel()
	maintCtx, maintCancel := context.WithCancel(context.Background())
	defer maintCancel()

	schedDone := make(chan struct{})
	g, gctx := errgroup.WithContext(ctx)

	if cfg.SchedulerEnabled {
		g.Go(func() error {
			defer close(schedDone)
			log.InfoContext(schedCtx, "scheduler starting", "tick", cfg.SchedulerTick, "instance_id", cfg.InstanceID)
			err := scheduler.Run(schedCtx)
			if err == nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("scheduler: %w", err)
		})
	} else {
		close(schedDone)
		log.InfoContext(ctx, "scheduler disabled")
	}

	g.Go(func() error {
		log.InfoContext(ctx, "http server listening", "addr", cfg.HTTPAddr, "storage", cfg.Storage)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http listen: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		log.InfoContext(ctx, "grpc server listening", "addr", cfg.GRPCAddr, "reflection", cfg.GRPCReflection)
		err := grpcSrv.Serve(grpcLis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		log.InfoContext(ctx, "admin server listening", "addr", cfg.AdminAddr, "version", version, "commit", commit, "date", date)
		err := adminSrv.Serve(nil)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("admin listen: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		err := maint.Run(maintCtx)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("maintenance: %w", err)
	})
	g.Go(func() error {
		<-gctx.Done()
		// 1: stop scheduler (resign leadership).
		log.InfoContext(gctx, "stopping scheduler")
		schedCancel()
		select {
		case <-schedDone:
		case <-time.After(5 * time.Second):
			log.WarnContext(gctx, "scheduler did not stop within 5s; continuing shutdown")
		}

		// 2: stop accepting HTTP/gRPC/admin and drain.
		shCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), cfg.ShutdownTimeout)
		defer cancel()
		log.InfoContext(shCtx, "shutting down http, grpc, and admin servers", "timeout", cfg.ShutdownTimeout)
		var shutdownErr error
		if err := srv.Shutdown(shCtx); err != nil {
			shutdownErr = fmt.Errorf("http shutdown: %w", err)
		}
		if err := adminSrv.Shutdown(shCtx); err != nil && shutdownErr == nil {
			shutdownErr = fmt.Errorf("admin shutdown: %w", err)
		}
		adaptergrpc.GracefulStop(grpcSrv, cfg.ShutdownTimeout)
		_ = grpcLis.Close()

		// 3: stop maintenance (4–5: notifier + storage close in deferred cleanup).
		maintCancel()
		return shutdownErr
	})

	if err := g.Wait(); err != nil {
		return err
	}
	log.InfoContext(ctx, "server stopped")
	return nil
}

func registerStoragePoolMetrics(reg prometheus.Registerer, repo app.JobRepository) error {
	switch r := repo.(type) {
	case *postgres.Repository:
		if err := reg.Register(prom.NewPostgresPoolCollector(r.Pool())); err != nil {
			return fmt.Errorf("postgres pool metrics: %w", err)
		}
	case *redisstore.Repository:
		if err := reg.Register(prom.NewRedisPoolCollector(r.Client())); err != nil {
			return fmt.Errorf("redis pool metrics: %w", err)
		}
	}
	return nil
}

func newRetryPolicy(cfg config.Config) app.RetryPolicy {
	switch cfg.RetryPolicy {
	case config.RetryPolicyFixed:
		return app.FixedDelayRetryPolicy{Delay: cfg.RetryBase}
	default:
		return app.ExponentialJitterPolicy{
			Base: cfg.RetryBase,
			Max:  cfg.RetryMax,
			Rand: rand.Float64,
		}
	}
}

func openRepository(ctx context.Context, cfg config.Config, log *slog.Logger) (app.JobRepository, func(), error) {
	switch cfg.Storage {
	case config.StoragePostgres:
		repo, err := postgres.Open(ctx, cfg.PostgresURL)
		if err != nil {
			return nil, nil, err
		}
		return repo, repo.Close, nil
	case config.StorageRedis:
		repo, err := redisstore.Open(ctx, cfg.RedisURL, cfg.RedisKeyPrefix)
		if err != nil {
			return nil, nil, err
		}
		return repo, func() {
			if err := repo.Close(); err != nil {
				log.ErrorContext(ctx, "redis close", "err", err)
			}
		}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported storage %q", cfg.Storage)
	}
}

func newLeaderElector(repo app.JobRepository, cfg config.Config) (app.LeaderElector, error) {
	switch r := repo.(type) {
	case *postgres.Repository:
		return postgres.NewLeaderElector(r.Pool()), nil
	case *redisstore.Repository:
		return redisstore.NewLeaderElector(r.Client(), r.KeyPrefix()).WithInstanceID(cfg.InstanceID), nil
	default:
		return nil, fmt.Errorf("no leader elector for repository type %T", repo)
	}
}

// mustQueueRepo asserts the job repository also implements QueueRepository (all backends do).
func mustQueueRepo(repo app.JobRepository) app.QueueRepository {
	q, ok := repo.(app.QueueRepository)
	if !ok {
		panic(fmt.Sprintf("%T does not implement app.QueueRepository", repo))
	}
	return q
}

func mustScheduleRepo(repo app.JobRepository) app.ScheduleRepository {
	s, ok := repo.(app.ScheduleRepository)
	if !ok {
		panic(fmt.Sprintf("%T does not implement app.ScheduleRepository", repo))
	}
	return s
}

func mustAPIKeyRepo(repo app.JobRepository) app.APIKeyRepository {
	k, ok := repo.(app.APIKeyRepository)
	if !ok {
		panic(fmt.Sprintf("%T does not implement app.APIKeyRepository", repo))
	}
	return k
}

func warnAuthConfig(ctx context.Context, log *slog.Logger, bootstrap string, keys app.APIKeyRepository) {
	stored, err := keys.ListAPIKeys(ctx)
	if err != nil {
		log.WarnContext(ctx, "unable to list API keys for auth startup check", "err", err)
		return
	}
	hasKeys := len(stored) > 0
	if bootstrap == "" && !hasKeys {
		log.WarnContext(ctx, "auth disabled: RELAY_API_KEY is empty and no stored API keys exist")
		return
	}
	if bootstrap != "" && hasKeys {
		log.WarnContext(ctx, "RELAY_API_KEY bootstrap is set alongside stored API keys")
	}
}

func mustMaintenanceRepo(repo app.JobRepository) app.MaintenanceRepository {
	m, ok := repo.(app.MaintenanceRepository)
	if !ok {
		panic(fmt.Sprintf("%T does not implement app.MaintenanceRepository", repo))
	}
	return m
}

func mustNotifier(repo app.JobRepository) app.JobNotifier {
	n, ok := repo.(app.JobNotifier)
	if !ok {
		panic(fmt.Sprintf("%T does not implement app.JobNotifier", repo))
	}
	return n
}

func migrateOnStart(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	switch cfg.Storage {
	case config.StoragePostgres:
		log.InfoContext(ctx, "running migrations on start")
		return postgres.Migrate(ctx, cfg.PostgresURL)
	case config.StorageRedis:
		log.InfoContext(ctx, "migrate on start skipped for redis storage")
		return nil
	default:
		return fmt.Errorf("unsupported storage %q", cfg.Storage)
	}
}

func runMigrate(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: relay-server migrate <up|down|status>")
	}
	action := args[0]

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	if cfg.Storage == config.StorageRedis {
		log.Info("migrate is a no-op for redis storage", "action", action)
		return nil
	}
	if cfg.Storage != config.StoragePostgres {
		return fmt.Errorf("migrate requires RELAY_STORAGE=postgres")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch action {
	case "up":
		return postgres.Migrate(ctx, cfg.PostgresURL)
	case "down":
		return postgres.MigrateDown(ctx, cfg.PostgresURL)
	case "status":
		statuses, err := postgres.MigrateStatus(ctx, cfg.PostgresURL)
		if err != nil {
			return err
		}
		for _, s := range statuses {
			appliedAt := ""
			if !s.AppliedAt.IsZero() {
				appliedAt = s.AppliedAt.UTC().Format(time.RFC3339)
			}
			fmt.Printf("%d\t%s\t%s\t%s\n", s.Source.Version, s.Source.Path, s.State, appliedAt)
		}
		return nil
	default:
		return fmt.Errorf("unknown migrate action %q (want up|down|status)", action)
	}
}

func runHealthcheck() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	hostport := cfg.HTTPAddr
	if strings.HasPrefix(hostport, ":") {
		hostport = net.JoinHostPort("127.0.0.1", strings.TrimPrefix(hostport, ":"))
	}
	url := "http://" + hostport + "/readyz"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s returned %d", url, resp.StatusCode)
	}
	return nil
}
