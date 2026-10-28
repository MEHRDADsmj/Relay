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
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	adapterhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/postgres"
	redisstore "github.com/MEHRDADsmj/relay/internal/adapter/storage/redis"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
	"github.com/MEHRDADsmj/relay/internal/platform/config"
	"github.com/MEHRDADsmj/relay/internal/platform/logging"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "relay-server: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd, rest := parseCommand(args)
	switch cmd {
	case "serve", "":
		return runServe()
	case "migrate":
		return runMigrate(rest)
	case "healthcheck":
		return runHealthcheck()
	default:
		return fmt.Errorf("unknown command %q (want serve|migrate|healthcheck)", cmd)
	}
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

	if cfg.APIKey == "" {
		log.Warn("RELAY_API_KEY is empty; HTTP auth is disabled")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	clock := app.SystemClock{}
	notifier := mustNotifier(repo)
	svc := app.NewJobService(
		repo,
		clock,
		app.UUIDv7Generator{},
		newRetryPolicy(cfg),
		log,
		notifier,
	)
	queues := app.NewQueueService(mustQueueRepo(repo), repo, clock)
	handler := adapterhttp.NewRouter(svc, queues, adapterhttp.Options{
		APIKey: cfg.APIKey,
		Logger: log,
	})

	maint := app.NewMaintenance(mustMaintenanceRepo(repo), clock, app.MaintenanceConfig{
		ReapInterval:  cfg.ReapInterval,
		ReapBatch:     cfg.ReapBatch,
		PurgeInterval: cfg.PurgeInterval,
		PurgeBatch:    cfg.PurgeBatch,
		Retention:     cfg.Retention,
	}, log)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// WriteTimeout must exceed max lease wait_seconds; see phase-2 §5.
		WriteTimeout: cfg.LeaseMaxWait + 15*time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Maintenance uses a child context cancelled only after HTTP shutdown so
	// long-polls can finish before the reaper/purger stops (phase-2 §7).
	maintCtx, maintCancel := context.WithCancel(context.Background())
	defer maintCancel()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("http server listening", "addr", cfg.HTTPAddr, "storage", cfg.Storage)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen: %w", err)
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
		// 1–2: stop accepting HTTP; Shutdown waits for in-flight handlers (incl. long-polls).
		shCtx, cancel := context.WithTimeout(context.WithoutCancel(gctx), cfg.ShutdownTimeout)
		defer cancel()
		log.Info("shutting down http server", "timeout", cfg.ShutdownTimeout)
		if err := srv.Shutdown(shCtx); err != nil {
			maintCancel()
			return fmt.Errorf("shutdown: %w", err)
		}
		// 3: stop maintenance (4–5: notifier + storage close in deferred cleanup).
		maintCancel()
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("server stopped")
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
				log.Error("redis close", "err", err)
			}
		}, nil
	default:
		return nil, nil, fmt.Errorf("unsupported storage %q", cfg.Storage)
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
		log.Info("running migrations on start")
		return postgres.Migrate(ctx, cfg.PostgresURL)
	case config.StorageRedis:
		log.Info("migrate on start skipped for redis storage")
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
