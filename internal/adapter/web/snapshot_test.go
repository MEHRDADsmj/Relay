package web_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-h/templ"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/web/templates"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestSnapshot_OverviewLive(t *testing.T) {
	t.Parallel()
	data := templates.OverviewData{
		CSRF:       "csrf-stable",
		KeyName:    "bootstrap",
		InstanceID: "relay-1",
		IsLeader:   true,
		Totals:     templates.QueueTotals{Pending: 2, Scheduled: 1, Running: 0, Dead: 0},
		Queues: []templates.QueueRow{{
			Stats: domain.QueueStats{
				Queue: "orders", Pending: 2, Scheduled: 1, OldestPendingAge: time.Minute,
			},
			Sparkline: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 28" width="120" height="28" role="img" aria-hidden="true"><path d="M2.0,26.0 L118.0,2.0" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>`,
		}},
	}
	assertSnapshot(t, "overview_live.html", templates.OverviewLive(data))
}

func TestSnapshot_QueueJobsEmptyDead(t *testing.T) {
	t.Parallel()
	data := templates.QueueDetailData{
		CSRF:   "csrf-stable",
		Queue:  "orders",
		Status: domain.StatusDead,
		Stats:  domain.QueueStats{Queue: "orders"},
		Window: "15m",
	}
	assertSnapshot(t, "queue_jobs_empty_dead.html", templates.QueueJobsFragment(data))
}

func TestSnapshot_JobPayloadEscaped(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("018f0000-0000-7000-8000-0000000000aa")
	data := templates.JobDetailData{
		CSRF: "csrf-stable",
		Job: domain.Job{
			ID:        id,
			Queue:     "orders",
			Type:      "email",
			Status:    domain.StatusPending,
			Payload:   []byte(`{"x":"<script>alert(1)</script>"}`),
			CreatedAt: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
			RunAt:     time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		},
		Payload: templates.PrettyJSON([]byte(`{"x":"<script>alert(1)</script>"}`)),
	}
	var buf bytes.Buffer
	require.NoError(t, templates.JobPage(data).Render(context.Background(), &buf))
	out := buf.String()
	require.NotContains(t, out, "<script>alert(1)</script>")
	// encoding/json escapes < as \u003c; templ then HTML-escapes the payload text.
	require.Contains(t, out, `\u003cscript\u003e`)
	assertSnapshot(t, "job_payload_escaped.html", templates.JobPage(data))
}

func assertSnapshot(t *testing.T, name string, component templ.Component) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, component.Render(context.Background(), &buf))
	got := buf.Bytes()
	path := filepath.Join("testdata", "snapshots", name)
	if os.Getenv("UPDATE_SNAPSHOTS") == "1" {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, got, 0o600))
	}
	want, err := os.ReadFile(path) //nolint:gosec // G304: test golden path is fixed under testdata/
	require.NoError(t, err, "missing golden %s (run with UPDATE_SNAPSHOTS=1)", path)
	require.Equal(t, string(want), string(got))
}
