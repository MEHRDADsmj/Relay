// Package templates holds typed templ components for the Relay dashboard.
package templates

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/a-h/templ"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// SafePath marks a same-origin path as a safe URL for templ href/action attributes.
func SafePath(path string) templ.SafeURL {
	return templ.URL(path)
}

// RawHTML inserts trusted server-generated markup (e.g. sparklines).
func RawHTML(s string) templ.Component {
	return templ.Raw(s)
}

// FormatInt64 formats an int64 for display.
func FormatInt64(n int64) string {
	return strconv.FormatInt(n, 10)
}

// OverviewData is the overview page model.
type OverviewData struct {
	CSRF       string
	KeyName    string
	InstanceID string
	IsLeader   bool
	Queues     []QueueRow
	Totals     QueueTotals
}

// QueueTotals aggregates counts across queues.
type QueueTotals struct {
	Pending   int64
	Scheduled int64
	Running   int64
	Dead      int64
}

// QueueRow is one overview table row.
type QueueRow struct {
	Stats      domain.QueueStats
	Sparkline  string // inline SVG path
	SparkTitle string
}

// QueueDetailData is the queue detail page.
type QueueDetailData struct {
	CSRF       string
	Queue      string
	Stats      domain.QueueStats
	Window     string // 15m|1h|24h
	ChartSVG   string
	Status     domain.Status
	Jobs       []domain.Job
	NextCursor string
	CanAdmin   bool
}

// JobDetailData is the job detail page.
type JobDetailData struct {
	CSRF      string
	Job       domain.Job
	Payload   string
	Attempts  []domain.Attempt
	TraceURL  string
	CanMutate bool
	CanCancel bool
	CanRetry  bool
}

// SchedulesData is the schedules list page.
type SchedulesData struct {
	CSRF      string
	Schedules []ScheduleRow
	CanAdmin  bool
}

// ScheduleRow is one schedule table row.
type ScheduleRow struct {
	Schedule domain.Schedule
	CronDesc string
}

// ScheduleDetailData is the schedule detail page.
type ScheduleDetailData struct {
	CSRF     string
	Schedule domain.Schedule
	CronDesc string
	Jobs     []domain.Job
	CanAdmin bool
}

// PrettyJSON returns indented JSON or the raw string if invalid.
func PrettyJSON(raw []byte) string {
	if len(raw) == 0 {
		return "{}"
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return string(raw)
	}
	return string(b)
}

// FormatAge renders a duration for display.
func FormatAge(d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	if d < time.Second {
		return "<1s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

// FormatTime renders a UTC timestamp.
func FormatTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

// FormatTimePtr renders an optional timestamp.
func FormatTimePtr(t *time.Time) string {
	if t == nil {
		return "—"
	}
	return FormatTime(*t)
}

// DescribeCron returns a short human-readable cron description.
func DescribeCron(expr string) string {
	switch expr {
	case "@yearly", "@annually":
		return "Once a year"
	case "@monthly":
		return "Once a month"
	case "@weekly":
		return "Once a week"
	case "@daily", "@midnight":
		return "Once a day"
	case "@hourly":
		return "Every hour"
	default:
		if len(expr) > 7 && expr[:6] == "@every" {
			return "Every " + expr[7:]
		}
		return "Cron: " + expr
	}
}

// SparklineSVG builds a simple polyline sparkline from throughput points (enqueued+succeeded).
func SparklineSVG(points []domain.ThroughputPoint, width, height int) string {
	if len(points) == 0 {
		return ""
	}
	vals := make([]float64, len(points))
	var max float64
	for i, p := range points {
		v := float64(p.Enqueued + p.Succeeded)
		vals[i] = v
		if v > max {
			max = v
		}
	}
	if max <= 0 {
		max = 1
	}
	pad := 2.0
	w := float64(width) - pad*2
	h := float64(height) - pad*2
	step := w / float64(len(vals)-1)
	if len(vals) == 1 {
		step = 0
	}
	path := ""
	for i, v := range vals {
		x := pad + float64(i)*step
		y := pad + h - (v/max)*h
		if i == 0 {
			path = fmt.Sprintf("M%.1f,%.1f", x, y)
		} else {
			path += fmt.Sprintf(" L%.1f,%.1f", x, y)
		}
	}
	return fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-hidden="true"><path d="%s" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>`,
		width, height, width, height, path,
	)
}

// ThroughputChartSVG builds a larger chart for queue detail.
func ThroughputChartSVG(points []domain.ThroughputPoint) string {
	return SparklineSVG(points, 640, 120)
}
