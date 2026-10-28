package main

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

type outputMode string

const (
	outputTable outputMode = "table"
	outputJSON  outputMode = "json"
)

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func newTable(w io.Writer) *tabwriter.Writer {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	return tw
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatTime(*t)
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	return d.String()
}

func writeKVTable(w io.Writer, rows [][2]string) error {
	tw := newTable(w)
	for _, r := range rows {
		if _, err := fmt.Fprintf(tw, "%s\t%s\n", r[0], r[1]); err != nil {
			return err
		}
	}
	return tw.Flush()
}
