package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Report is the full benchmark output (text or JSON).
type Report struct {
	Environment Environment      `json:"environment"`
	Config      ReportConfig     `json:"config"`
	Enqueue     ThroughputBlock  `json:"enqueue"`
	Processing  ThroughputBlock  `json:"processing"`
	EndToEnd    LatencySummary   `json:"end_to_end_latency"`
	Errors      map[string]int64 `json:"errors"`
	Completed   int64            `json:"completed"`
	MeasuredAt  time.Time        `json:"measured_at"`
}

// ReportConfig echoes the run knobs.
type ReportConfig struct {
	Server       string `json:"server"`
	Transport    string `json:"transport"`
	Queue        string `json:"queue"`
	Jobs         int    `json:"jobs"`
	Producers    int    `json:"producers"`
	Workers      int    `json:"workers"`
	Concurrency  int    `json:"concurrency"`
	PayloadBytes int    `json:"payload_bytes"`
	HandlerSleep string `json:"handler_sleep"`
	Warmup       string `json:"warmup"`
}

// ThroughputBlock is count + rate (+ optional latency for enqueue).
type ThroughputBlock struct {
	Count      int64          `json:"count"`
	Wall       string         `json:"wall"`
	JobsPerSec float64        `json:"jobs_per_sec"`
	Latency    LatencySummary `json:"latency,omitempty"`
}

// LatencySummary is HDR histogram percentiles.
type LatencySummary struct {
	Count int64  `json:"count"`
	P50   string `json:"p50"`
	P90   string `json:"p90"`
	P99   string `json:"p99"`
	Max   string `json:"max"`
	P50Ns int64  `json:"p50_ns"`
	P90Ns int64  `json:"p90_ns"`
	P99Ns int64  `json:"p99_ns"`
	MaxNs int64  `json:"max_ns"`
}

func histSummary(h *hdrhistogram.Histogram) LatencySummary {
	if h == nil || h.TotalCount() == 0 {
		return LatencySummary{}
	}
	p50 := h.ValueAtQuantile(50)
	p90 := h.ValueAtQuantile(90)
	p99 := h.ValueAtQuantile(99)
	max := h.Max()
	return LatencySummary{
		Count: h.TotalCount(),
		P50:   time.Duration(p50).String(),
		P90:   time.Duration(p90).String(),
		P99:   time.Duration(p99).String(),
		Max:   time.Duration(max).String(),
		P50Ns: p50,
		P90Ns: p90,
		P99Ns: p99,
		MaxNs: max,
	}
}

func writeReport(w io.Writer, format string, rep Report) error {
	switch format {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	default:
		return writeTextReport(w, rep)
	}
}

func writeTextReport(w io.Writer, rep Report) error {
	var b strings.Builder
	b.WriteString("=== Relay benchmark report ===\n")
	fmt.Fprintf(&b, "measured_at: %s\n", rep.MeasuredAt.Format(time.RFC3339))
	b.WriteString("\n-- Environment --\n")
	fmt.Fprintf(&b, "go_version:      %s\n", rep.Environment.GoVersion)
	fmt.Fprintf(&b, "gomaxprocs:      %d\n", rep.Environment.GOMAXPROCS)
	fmt.Fprintf(&b, "cpu_model:       %s\n", rep.Environment.CPUModel)
	fmt.Fprintf(&b, "storage_backend: %s\n", rep.Environment.StorageBackend)
	fmt.Fprintf(&b, "server_version:  %s\n", rep.Environment.ServerVersion)
	fmt.Fprintf(&b, "server_commit:   %s\n", rep.Environment.ServerCommit)
	fmt.Fprintf(&b, "server_go:       %s\n", rep.Environment.ServerGoVersion)
	b.WriteString("\n-- Config --\n")
	fmt.Fprintf(&b, "server:          %s\n", rep.Config.Server)
	fmt.Fprintf(&b, "transport:       %s\n", rep.Config.Transport)
	fmt.Fprintf(&b, "queue:           %s\n", rep.Config.Queue)
	fmt.Fprintf(&b, "jobs:            %d\n", rep.Config.Jobs)
	fmt.Fprintf(&b, "producers:       %d\n", rep.Config.Producers)
	fmt.Fprintf(&b, "workers:         %d\n", rep.Config.Workers)
	fmt.Fprintf(&b, "concurrency:     %d\n", rep.Config.Concurrency)
	fmt.Fprintf(&b, "payload_bytes:   %d\n", rep.Config.PayloadBytes)
	fmt.Fprintf(&b, "handler_sleep:   %s\n", rep.Config.HandlerSleep)
	fmt.Fprintf(&b, "warmup:          %s\n", rep.Config.Warmup)
	b.WriteString("\n-- Enqueue --\n")
	fmt.Fprintf(&b, "count:           %d\n", rep.Enqueue.Count)
	fmt.Fprintf(&b, "wall:            %s\n", rep.Enqueue.Wall)
	fmt.Fprintf(&b, "jobs/s:          %.2f\n", rep.Enqueue.JobsPerSec)
	writeLatency(&b, "enqueue_latency", rep.Enqueue.Latency)
	b.WriteString("\n-- Processing --\n")
	fmt.Fprintf(&b, "completed:       %d\n", rep.Processing.Count)
	fmt.Fprintf(&b, "wall (enqueue→drain): %s\n", rep.Processing.Wall)
	fmt.Fprintf(&b, "completions/s:   %.2f\n", rep.Processing.JobsPerSec)
	b.WriteString("\n-- End-to-end latency (enqueue accepted → handler start) --\n")
	writeLatency(&b, "e2e", rep.EndToEnd)
	if len(rep.Errors) > 0 {
		b.WriteString("\n-- Errors --\n")
		for k, v := range rep.Errors {
			fmt.Fprintf(&b, "%s: %d\n", k, v)
		}
	}
	b.WriteString("\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func writeLatency(b *strings.Builder, label string, lat LatencySummary) {
	if lat.Count == 0 {
		fmt.Fprintf(b, "%s:           (no samples)\n", label)
		return
	}
	fmt.Fprintf(b, "%s_count:  %d\n", label, lat.Count)
	fmt.Fprintf(b, "%s_p50:    %s\n", label, lat.P50)
	fmt.Fprintf(b, "%s_p90:    %s\n", label, lat.P90)
	fmt.Fprintf(b, "%s_p99:    %s\n", label, lat.P99)
	fmt.Fprintf(b, "%s_max:    %s\n", label, lat.Max)
}
