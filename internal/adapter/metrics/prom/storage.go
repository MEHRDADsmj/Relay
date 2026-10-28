package prom

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

// PostgresPoolCollector exports pgxpool stats as relay_storage_pool_* gauges.
type PostgresPoolCollector struct {
	pool *pgxpool.Pool

	acquired *prometheus.Desc
	idle     *prometheus.Desc
	total    *prometheus.Desc
	maxConns *prometheus.Desc
	emptyHit *prometheus.Desc
}

// NewPostgresPoolCollector builds a collector for pool.
func NewPostgresPoolCollector(pool *pgxpool.Pool) *PostgresPoolCollector {
	return &PostgresPoolCollector{
		pool:     pool,
		acquired: prometheus.NewDesc("relay_storage_pool_acquired", "Acquired connections in the pool.", nil, prometheus.Labels{"backend": "postgres"}),
		idle:     prometheus.NewDesc("relay_storage_pool_idle", "Idle connections in the pool.", nil, prometheus.Labels{"backend": "postgres"}),
		total:    prometheus.NewDesc("relay_storage_pool_total", "Total connections in the pool.", nil, prometheus.Labels{"backend": "postgres"}),
		maxConns: prometheus.NewDesc("relay_storage_pool_max_conns", "Configured max connections.", nil, prometheus.Labels{"backend": "postgres"}),
		emptyHit: prometheus.NewDesc("relay_storage_pool_empty_acquire_total", "Times a connection was needed but the pool was empty.", nil, prometheus.Labels{"backend": "postgres"}),
	}
}

func (c *PostgresPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquired
	ch <- c.idle
	ch <- c.total
	ch <- c.maxConns
	ch <- c.emptyHit
}

func (c *PostgresPoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.pool == nil {
		return
	}
	st := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(st.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(st.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(st.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(st.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.emptyHit, prometheus.CounterValue, float64(st.EmptyAcquireCount()))
}

// RedisPoolCollector exports go-redis pool stats as relay_storage_pool_* gauges.
type RedisPoolCollector struct {
	client *goredis.Client

	hits     *prometheus.Desc
	misses   *prometheus.Desc
	timeouts *prometheus.Desc
	total    *prometheus.Desc
	idle     *prometheus.Desc
	stale    *prometheus.Desc
}

// NewRedisPoolCollector builds a collector for client.
func NewRedisPoolCollector(client *goredis.Client) *RedisPoolCollector {
	return &RedisPoolCollector{
		client:   client,
		hits:     prometheus.NewDesc("relay_storage_pool_hits_total", "Free connection hits.", nil, prometheus.Labels{"backend": "redis"}),
		misses:   prometheus.NewDesc("relay_storage_pool_misses_total", "Free connection misses.", nil, prometheus.Labels{"backend": "redis"}),
		timeouts: prometheus.NewDesc("relay_storage_pool_timeouts_total", "Wait timeouts.", nil, prometheus.Labels{"backend": "redis"}),
		total:    prometheus.NewDesc("relay_storage_pool_total", "Total connections in the pool.", nil, prometheus.Labels{"backend": "redis"}),
		idle:     prometheus.NewDesc("relay_storage_pool_idle", "Idle connections in the pool.", nil, prometheus.Labels{"backend": "redis"}),
		stale:    prometheus.NewDesc("relay_storage_pool_stale_total", "Stale connections removed.", nil, prometheus.Labels{"backend": "redis"}),
	}
}

func (c *RedisPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hits
	ch <- c.misses
	ch <- c.timeouts
	ch <- c.total
	ch <- c.idle
	ch <- c.stale
}

func (c *RedisPoolCollector) Collect(ch chan<- prometheus.Metric) {
	if c.client == nil {
		return
	}
	st := c.client.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(st.Hits))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(st.Misses))
	ch <- prometheus.MustNewConstMetric(c.timeouts, prometheus.CounterValue, float64(st.Timeouts))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(st.TotalConns))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(st.IdleConns))
	ch <- prometheus.MustNewConstMetric(c.stale, prometheus.CounterValue, float64(st.StaleConns))
}
