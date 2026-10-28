package web_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/web/templates"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

// Generates standalone HTML for docs/media screenshots (UPDATE_SNAPSHOTS=1).
func TestGenerateScreenshotHTML(t *testing.T) {
	if os.Getenv("UPDATE_SNAPSHOTS") != "1" {
		t.Skip("set UPDATE_SNAPSHOTS=1 to regenerate screenshot HTML")
	}
	css, err := os.ReadFile(filepath.Join("static", "app.css"))
	require.NoError(t, err)

	data := templates.OverviewData{
		CSRF:       "csrf",
		KeyName:    "bootstrap",
		InstanceID: "relay-dev-1",
		IsLeader:   true,
		Totals:     templates.QueueTotals{Pending: 12, Scheduled: 3, Running: 2, Dead: 1},
		Queues: []templates.QueueRow{
			{
				Stats: domain.QueueStats{
					Queue: "orders", Pending: 8, Scheduled: 2, Running: 1, Dead: 1,
					OldestPendingAge: 90 * time.Second, Paused: false,
				},
				Sparkline: templates.SparklineSVG([]domain.ThroughputPoint{
					{Enqueued: 1}, {Enqueued: 3}, {Enqueued: 2}, {Enqueued: 5}, {Enqueued: 4},
				}, 120, 28),
			},
			{
				Stats: domain.QueueStats{
					Queue: "emails", Pending: 4, Scheduled: 1, Running: 1, Dead: 0,
					OldestPendingAge: 15 * time.Second, Paused: true,
				},
				Sparkline: templates.SparklineSVG([]domain.ThroughputPoint{
					{Enqueued: 2}, {Enqueued: 2}, {Enqueued: 1}, {Enqueued: 0}, {Enqueued: 3},
				}, 120, 28),
			},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, templates.HomePage(data).Render(context.Background(), &buf))
	html := buf.String()
	html = strings.Replace(html, `<link rel="stylesheet" href="/ui/static/app.css">`,
		"<style>\n"+string(css)+"\n</style>", 1)
	html = strings.ReplaceAll(html, `<script src="/ui/static/htmx.min.js" defer></script>`, "")
	html = strings.ReplaceAll(html, `<script src="/ui/static/htmx-ext-sse.js" defer></script>`, "")

	outDir := filepath.Join("..", "..", "..", "docs", "media")
	require.NoError(t, os.MkdirAll(outDir, 0o750))
	out := filepath.Join(outDir, "dashboard-overview.html")
	require.NoError(t, os.WriteFile(out, []byte(html), 0o600))
	t.Log("wrote", out)
}
