// Package sdkprom provides Prometheus Hooks for the Relay SDK worker.
package sdkprom

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

var jobDurationBuckets = []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900}

// Option configures NewHooks.
type Option func(*options)

type options struct {
	constLabels prometheus.Labels
}

// WithConstLabels adds constant labels to all metrics.
func WithConstLabels(labels prometheus.Labels) Option {
	return func(o *options) {
		o.constLabels = labels
	}
}

type hooks struct {
	inFlight  *prometheus.GaugeVec
	duration  *prometheus.HistogramVec
	jobsTotal *prometheus.CounterVec
	leaseErrs *prometheus.CounterVec
}

// NewHooks registers worker metrics on reg and returns sdk.Hooks.
func NewHooks(reg prometheus.Registerer, opts ...Option) sdk.Hooks {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	h := &hooks{
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "relay",
			Name:        "worker_jobs_in_flight",
			Help:        "Jobs currently being processed by this worker.",
			ConstLabels: o.constLabels,
		}, []string{"queue"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "relay",
			Name:        "worker_job_duration_seconds",
			Help:        "Handler wall time per job.",
			Buckets:     jobDurationBuckets,
			ConstLabels: o.constLabels,
		}, []string{"queue", "job_type"}),
		jobsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "relay",
			Name:        "worker_jobs_total",
			Help:        "Jobs finished by this worker.",
			ConstLabels: o.constLabels,
		}, []string{"queue", "job_type", "outcome"}),
		leaseErrs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "relay",
			Name:        "worker_lease_errors_total",
			Help:        "Lease RPC errors observed by this worker.",
			ConstLabels: o.constLabels,
		}, []string{"queue"}),
	}

	reg.MustRegister(h.inFlight, h.duration, h.jobsTotal, h.leaseErrs)
	return h
}

func (h *hooks) OnLease(queue string, _ int, _ time.Duration, err error) {
	if err != nil {
		h.leaseErrs.WithLabelValues(queue).Inc()
	}
}

func (h *hooks) OnJobStart(queue, _ string) {
	h.inFlight.WithLabelValues(queue).Inc()
}

func (h *hooks) OnJobEnd(queue, jobType string, d time.Duration, outcome string) {
	h.inFlight.WithLabelValues(queue).Dec()
	h.duration.WithLabelValues(queue, jobType).Observe(d.Seconds())
	if outcome == "" {
		outcome = "failed"
	}
	h.jobsTotal.WithLabelValues(queue, jobType, outcome).Inc()
}

func (h *hooks) OnHeartbeat(string, error) {}
