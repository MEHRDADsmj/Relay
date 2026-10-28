package app

import (
	"time"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Metrics is the app-layer port for business metrics (vendor-free).
type Metrics interface {
	JobEnqueued(queue, jobType string, created bool)
	JobLeased(queue string, n int, waited time.Duration)
	JobFinished(queue, jobType string, outcome domain.AttemptOutcome, runDuration time.Duration)
	JobDead(queue, jobType string)
	LeaseLost(queue string)
	SchedulerFired(schedule string, ok bool)
	LeaderChanged(name string, isLeader bool)
	MaintenanceRun(kind string, affected int, d time.Duration, err error)
}

// NoopMetrics discards all metric observations.
type NoopMetrics struct{}

func (NoopMetrics) JobEnqueued(string, string, bool)     {}
func (NoopMetrics) JobLeased(string, int, time.Duration) {}
func (NoopMetrics) JobFinished(string, string, domain.AttemptOutcome, time.Duration) {
}
func (NoopMetrics) JobDead(string, string)                           {}
func (NoopMetrics) LeaseLost(string)                                 {}
func (NoopMetrics) SchedulerFired(string, bool)                      {}
func (NoopMetrics) LeaderChanged(string, bool)                       {}
func (NoopMetrics) MaintenanceRun(string, int, time.Duration, error) {}

func metricsOrNoop(m Metrics) Metrics {
	if m == nil {
		return NoopMetrics{}
	}
	return m
}
