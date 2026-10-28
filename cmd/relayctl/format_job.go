package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func jobDTO(j sdk.JobInfo) map[string]any {
	m := map[string]any{
		"id":               j.ID,
		"queue":            j.Queue,
		"type":             j.Type,
		"status":           j.Status,
		"priority":         j.Priority,
		"run_at":           formatTime(j.RunAt),
		"attempts":         j.Attempts,
		"max_attempts":     j.MaxAttempts,
		"idempotency_key":  j.IdempotencyKey,
		"last_error":       j.LastError,
		"cancel_requested": j.CancelRequested,
		"created_at":       formatTime(j.CreatedAt),
		"updated_at":       formatTime(j.UpdatedAt),
	}
	if len(j.Payload) > 0 {
		m["payload"] = jsonRawOrString(j.Payload)
	}
	if j.LeaseOwner != "" {
		m["lease_owner"] = j.LeaseOwner
	}
	if j.LeaseExpiresAt != nil {
		m["lease_expires_at"] = formatTimePtr(j.LeaseExpiresAt)
	}
	if j.CancelledAt != nil {
		m["cancelled_at"] = formatTimePtr(j.CancelledAt)
	}
	if j.FinishedAt != nil {
		m["finished_at"] = formatTimePtr(j.FinishedAt)
	}
	if len(j.TraceContext) > 0 {
		m["trace_context"] = j.TraceContext
	}
	return m
}

func jsonRawOrString(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return string(b)
	}
	return v
}

func writeJob(w io.Writer, mode outputMode, j sdk.JobInfo) error {
	if mode == outputJSON {
		return writeJSON(w, jobDTO(j))
	}
	return writeKVTable(w, [][2]string{
		{"ID", j.ID},
		{"Queue", j.Queue},
		{"Type", j.Type},
		{"Status", j.Status},
		{"Priority", fmt.Sprintf("%d", j.Priority)},
		{"RunAt", formatTime(j.RunAt)},
		{"Attempts", fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts)},
		{"IdempotencyKey", j.IdempotencyKey},
		{"CreatedAt", formatTime(j.CreatedAt)},
		{"UpdatedAt", formatTime(j.UpdatedAt)},
	})
}

func writeJobs(w io.Writer, mode outputMode, jobs []sdk.JobInfo) error {
	if mode == outputJSON {
		out := make([]map[string]any, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, jobDTO(j))
		}
		return writeJSON(w, out)
	}
	tw := newTable(w)
	if _, err := fmt.Fprintln(tw, "ID\tQUEUE\tTYPE\tSTATUS\tPRIORITY\tATTEMPTS\tRUN_AT"); err != nil {
		return err
	}
	for _, j := range jobs {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d/%d\t%s\n",
			j.ID, j.Queue, j.Type, j.Status, j.Priority, j.Attempts, j.MaxAttempts, formatTime(j.RunAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAttempts(w io.Writer, mode outputMode, attempts []sdk.AttemptInfo) error {
	if mode == outputJSON {
		out := make([]map[string]any, 0, len(attempts))
		for _, a := range attempts {
			out = append(out, map[string]any{
				"job_id":      a.JobID,
				"number":      a.Number,
				"worker_id":   a.WorkerID,
				"started_at":  formatTime(a.StartedAt),
				"finished_at": formatTimePtr(a.FinishedAt),
				"outcome":     a.Outcome,
				"error":       a.Error,
			})
		}
		return writeJSON(w, out)
	}
	tw := newTable(w)
	if _, err := fmt.Fprintln(tw, "JOB_ID\tNUMBER\tWORKER\tOUTCOME\tSTARTED\tFINISHED\tERROR"); err != nil {
		return err
	}
	for _, a := range attempts {
		errMsg := strings.ReplaceAll(a.Error, "\n", " ")
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			a.JobID, a.Number, a.WorkerID, a.Outcome, formatTime(a.StartedAt), formatTimePtr(a.FinishedAt), errMsg); err != nil {
			return err
		}
	}
	return tw.Flush()
}
