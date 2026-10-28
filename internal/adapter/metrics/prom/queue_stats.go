package prom

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

const (
	defaultStatsCacheTTL = 10 * time.Second
	defaultStatsTimeout  = 2 * time.Second
)

// QueueStatsSource provides queue list + stats for the collector.
type QueueStatsSource interface {
	ListQueues(ctx context.Context) ([]domain.Queue, error)
	Stats(ctx context.Context, queue string) (domain.QueueStats, error)
}

// QueueStatsCollector is a custom Prometheus collector for queue gauges.
type QueueStatsCollector struct {
	source   QueueStatsSource
	cacheTTL time.Duration
	timeout  time.Duration
	clock    func() time.Time

	jobsDesc   *prometheus.Desc
	oldestDesc *prometheus.Desc
	pausedDesc *prometheus.Desc

	mu       sync.Mutex
	cachedAt time.Time
	cached   []domain.QueueStats

	CollectErrors prometheus.Counter
}

// QueueStatsOptions configures the collector.
type QueueStatsOptions struct {
	CacheTTL time.Duration
	Timeout  time.Duration
	Clock    func() time.Time
}

// NewQueueStatsCollector builds a collector. Register it and CollectErrors on a Registerer.
func NewQueueStatsCollector(source QueueStatsSource, opts QueueStatsOptions) *QueueStatsCollector {
	ttl := opts.CacheTTL
	if ttl <= 0 {
		ttl = defaultStatsCacheTTL
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultStatsTimeout
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	return &QueueStatsCollector{
		source:   source,
		cacheTTL: ttl,
		timeout:  timeout,
		clock:    clock,
		jobsDesc: prometheus.NewDesc(
			"relay_queue_jobs",
			"Jobs in a queue by status.",
			[]string{"queue", "status"}, nil,
		),
		oldestDesc: prometheus.NewDesc(
			"relay_queue_oldest_pending_age_seconds",
			"Age of the oldest pending job in the queue.",
			[]string{"queue"}, nil,
		),
		pausedDesc: prometheus.NewDesc(
			"relay_queue_paused",
			"1 if the queue is paused.",
			[]string{"queue"}, nil,
		),
		CollectErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "stats_collect_errors_total",
			Help:      "Queue stats scrape failures (timeout or source error).",
		}),
	}
}

// Describe implements prometheus.Collector.
func (c *QueueStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobsDesc
	ch <- c.oldestDesc
	ch <- c.pausedDesc
}

// Collect implements prometheus.Collector.
func (c *QueueStatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats, erred := c.snapshot()
	if erred {
		c.CollectErrors.Inc()
	}
	for _, st := range stats {
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, float64(st.Pending), st.Queue, "pending")
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, float64(st.Scheduled), st.Queue, "scheduled")
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, float64(st.Running), st.Queue, "running")
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, float64(st.Succeeded), st.Queue, "succeeded")
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, float64(st.Dead), st.Queue, "dead")
		ch <- prometheus.MustNewConstMetric(c.jobsDesc, prometheus.GaugeValue, float64(st.Cancelled), st.Queue, "cancelled")
		ch <- prometheus.MustNewConstMetric(c.oldestDesc, prometheus.GaugeValue, st.OldestPendingAge.Seconds(), st.Queue)
		paused := 0.0
		if st.Paused {
			paused = 1
		}
		ch <- prometheus.MustNewConstMetric(c.pausedDesc, prometheus.GaugeValue, paused, st.Queue)
	}
}

func (c *QueueStatsCollector) snapshot() ([]domain.QueueStats, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if len(c.cached) > 0 && now.Sub(c.cachedAt) < c.cacheTTL {
		out := make([]domain.QueueStats, len(c.cached))
		copy(out, c.cached)
		return out, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	ctx = app.WithPrincipal(ctx, app.SystemPrincipal)

	queues, err := c.source.ListQueues(ctx)
	if err != nil {
		out := make([]domain.QueueStats, len(c.cached))
		copy(out, c.cached)
		return out, true
	}
	out := make([]domain.QueueStats, 0, len(queues))
	for _, q := range queues {
		st, err := c.source.Stats(ctx, q.Name)
		if err != nil {
			cached := make([]domain.QueueStats, len(c.cached))
			copy(cached, c.cached)
			return cached, true
		}
		out = append(out, st)
	}
	c.cached = out
	c.cachedAt = now
	copied := make([]domain.QueueStats, len(out))
	copy(copied, out)
	return copied, false
}

// Ensure QueueService satisfies QueueStatsSource at compile time.
var _ QueueStatsSource = (*app.QueueService)(nil)
