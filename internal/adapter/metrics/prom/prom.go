// Package prom implements app.Metrics with Prometheus and related collectors.
package prom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

var (
	jobRunBuckets    = []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900}
	leaseWaitBuckets = []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30}
)

// Metrics is the Prometheus adapter for app.Metrics.
type Metrics struct {
	cap *seriesCap

	jobsEnqueued   *prometheus.CounterVec
	jobsLeased     *prometheus.CounterVec
	leaseWait      *prometheus.HistogramVec
	jobsFinished   *prometheus.CounterVec
	jobRunDuration *prometheus.HistogramVec
	jobsDead       *prometheus.CounterVec
	leaseLost      *prometheus.CounterVec
	schedulerFires *prometheus.CounterVec
	leader         *prometheus.GaugeVec
	maintRuns      *prometheus.CounterVec
	maintAffected  *prometheus.CounterVec
	maintDuration  *prometheus.HistogramVec
}

// Options configures the Prometheus metrics adapter.
type Options struct {
	// MaxSeries caps distinct (queue, job_type) pairs (default 1000).
	MaxSeries int
	// Registerer defaults to prometheus.DefaultRegisterer.
	Registerer prometheus.Registerer
}

// New registers Relay business metrics on reg and returns the adapter.
func New(opts Options) (*Metrics, error) {
	reg := opts.Registerer
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	max := opts.MaxSeries
	if max <= 0 {
		max = 1000
	}

	m := &Metrics{
		cap: newSeriesCap(max),
		jobsEnqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "jobs_enqueued_total",
			Help:      "Jobs enqueued (created or idempotent replay).",
		}, []string{"queue", "job_type", "result"}),
		jobsLeased: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "jobs_leased_total",
			Help:      "Jobs leased to workers.",
		}, []string{"queue"}),
		leaseWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "relay",
			Name:      "lease_wait_seconds",
			Help:      "Time spent waiting in LeaseWait before returning.",
			Buckets:   leaseWaitBuckets,
		}, []string{"queue"}),
		jobsFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "jobs_finished_total",
			Help:      "Jobs finished by workers (complete/fail outcomes).",
		}, []string{"queue", "job_type", "outcome"}),
		jobRunDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "relay",
			Name:      "job_run_duration_seconds",
			Help:      "Attempt run duration from start to finish.",
			Buckets:   jobRunBuckets,
		}, []string{"queue", "job_type"}),
		jobsDead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "jobs_dead_total",
			Help:      "Jobs marked dead after exhausting retries.",
		}, []string{"queue", "job_type"}),
		leaseLost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "lease_lost_total",
			Help:      "Lease ownership lost during worker operations.",
		}, []string{"queue"}),
		schedulerFires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "scheduler_fires_total",
			Help:      "Scheduler fire attempts.",
		}, []string{"schedule", "result"}),
		leader: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "relay",
			Name:      "leader",
			Help:      "1 if this instance holds leadership for name.",
		}, []string{"name"}),
		maintRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "maintenance_runs_total",
			Help:      "Maintenance loop iterations.",
		}, []string{"kind"}),
		maintAffected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "maintenance_affected_total",
			Help:      "Jobs affected by maintenance.",
		}, []string{"kind"}),
		maintDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "relay",
			Name:      "maintenance_duration_seconds",
			Help:      "Maintenance iteration duration.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"kind"}),
	}

	collectors := []prometheus.Collector{
		m.jobsEnqueued, m.jobsLeased, m.leaseWait, m.jobsFinished, m.jobRunDuration,
		m.jobsDead, m.leaseLost, m.schedulerFires, m.leader,
		m.maintRuns, m.maintAffected, m.maintDuration,
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Metrics) JobEnqueued(queue, jobType string, created bool) {
	result := "replayed"
	if created {
		result = "created"
	}
	jt := m.cap.jobType(queue, jobType)
	m.jobsEnqueued.WithLabelValues(queue, jt, result).Inc()
}

func (m *Metrics) JobLeased(queue string, n int, waited time.Duration) {
	if n > 0 {
		m.jobsLeased.WithLabelValues(queue).Add(float64(n))
	}
	m.leaseWait.WithLabelValues(queue).Observe(waited.Seconds())
}

func (m *Metrics) JobFinished(queue, jobType string, outcome domain.AttemptOutcome, runDuration time.Duration) {
	jt := m.cap.jobType(queue, jobType)
	m.jobsFinished.WithLabelValues(queue, jt, string(outcome)).Inc()
	if runDuration > 0 {
		m.jobRunDuration.WithLabelValues(queue, jt).Observe(runDuration.Seconds())
	}
}

func (m *Metrics) JobDead(queue, jobType string) {
	jt := m.cap.jobType(queue, jobType)
	m.jobsDead.WithLabelValues(queue, jt).Inc()
}

func (m *Metrics) LeaseLost(queue string) {
	m.leaseLost.WithLabelValues(queue).Inc()
}

func (m *Metrics) SchedulerFired(schedule string, ok bool) {
	result := "error"
	if ok {
		result = "ok"
	}
	m.schedulerFires.WithLabelValues(schedule, result).Inc()
}

func (m *Metrics) LeaderChanged(name string, isLeader bool) {
	v := 0.0
	if isLeader {
		v = 1
	}
	m.leader.WithLabelValues(name).Set(v)
}

func (m *Metrics) MaintenanceRun(kind string, affected int, d time.Duration, err error) {
	m.maintRuns.WithLabelValues(kind).Inc()
	if affected > 0 {
		m.maintAffected.WithLabelValues(kind).Add(float64(affected))
	}
	m.maintDuration.WithLabelValues(kind).Observe(d.Seconds())
	_ = err // duration/runs recorded regardless; errors still count as a run
}

// RegisterBuildInfo registers relay_build_info{version,commit,go_version,storage}=1.
func RegisterBuildInfo(reg prometheus.Registerer, version, commit, goVersion, storage string) error {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "relay",
		Name:      "build_info",
		Help:      "Build information (always 1).",
	}, []string{"version", "commit", "go_version", "storage"})
	if err := reg.Register(g); err != nil {
		return err
	}
	g.WithLabelValues(version, commit, goVersion, storage).Set(1)
	return nil
}

// RegisterRuntime registers Go and process collectors.
func RegisterRuntime(reg prometheus.Registerer) error {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return err
	}
	return reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}
