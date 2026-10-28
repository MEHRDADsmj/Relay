package admin_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/admin"
	adapterhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestAdmin_MetricsAndPprof(t *testing.T) {
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_metric_total"})
	require.NoError(t, reg.Register(counter))
	counter.Inc()

	srv := admin.New(admin.Options{Gatherer: reg})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	ctx := context.Background()
	requireBodyContains(t, ctx, ts.URL+"/metrics", "test_metric_total")
	requireStatus(t, ctx, ts.URL+"/debug/pprof/", http.StatusOK)
	requireStatus(t, ctx, ts.URL+"/healthz", http.StatusOK)
}

func requireBodyContains(t *testing.T, ctx context.Context, url, want string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), want)
}

func requireStatus(t *testing.T, ctx context.Context, url string, want int) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, want, res.StatusCode)
}

func TestPublicRouter_NoPprof(t *testing.T) {
	repo := memory.New()
	clock := app.SystemClock{}
	jobs := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Minute}, nil, nil)
	queues := app.NewQueueService(repo, repo, repo, clock)
	h := adapterhttp.NewRouter(jobs, queues, nil, nil, adapterhttp.Options{})

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.NotEqual(t, http.StatusOK, rec.Code, "pprof must not be on the public router")
	require.False(t, strings.Contains(rec.Body.String(), "types of profiles available"), "pprof index must not leak on public port")
}
