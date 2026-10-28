package web_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/net/html"

	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestUI_OverviewEmptyAndSeeded(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")

	empty := env.do(t, http.MethodGet, "/ui/", nil, cookies)
	require.Equal(t, http.StatusOK, empty.Code)
	assert.Contains(t, empty.Body.String(), "No queues yet")
	assert.Contains(t, empty.Body.String(), "test-instance")

	_, _, err := env.jobs.Enqueue(adminUICtx(), app.EnqueueCommand{Queue: "orders", Type: "ship", Payload: []byte(`{"n":1}`)})
	require.NoError(t, err)

	seeded := env.do(t, http.MethodGet, "/ui/", nil, cookies)
	require.Equal(t, http.StatusOK, seeded.Code)
	assert.Contains(t, seeded.Body.String(), "orders")
	assert.Contains(t, seeded.Body.String(), `href="/ui/queues/orders"`)
}

func TestUI_QueueDetailAndPause(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")
	_, _, err := env.jobs.Enqueue(adminUICtx(), app.EnqueueCommand{Queue: "q1", Type: "t", Payload: []byte(`{}`)})
	require.NoError(t, err)

	page := env.do(t, http.MethodGet, "/ui/queues/q1", nil, cookies)
	require.Equal(t, http.StatusOK, page.Code)
	assert.Contains(t, page.Body.String(), "q1")
	assert.Contains(t, page.Body.String(), "Pause")

	csrf := extractCSRF(t, page.Body.String())
	paused := env.do(t, http.MethodPost, "/ui/queues/q1/pause", url.Values{"csrf": {csrf}}, cookies)
	require.Equal(t, http.StatusSeeOther, paused.Code)
	cookies = mergeCookies(cookies, paused.Result().Cookies())

	again := env.do(t, http.MethodGet, "/ui/queues/q1", nil, cookies)
	require.Equal(t, http.StatusOK, again.Code)
	assert.Contains(t, again.Body.String(), "Resume")
	assert.Contains(t, again.Body.String(), "paused")
}

func TestUI_JobDetail(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")
	job, _, err := env.jobs.Enqueue(adminUICtx(), app.EnqueueCommand{
		Queue: "q1", Type: "email", Payload: []byte(`{"to":"a@b.c"}`),
	})
	require.NoError(t, err)

	page := env.do(t, http.MethodGet, "/ui/jobs/"+job.ID.String(), nil, cookies)
	require.Equal(t, http.StatusOK, page.Code)
	assert.Contains(t, page.Body.String(), job.ID.String())
	assert.Contains(t, page.Body.String(), "a@b.c")
	assert.Contains(t, page.Body.String(), "email")
	assert.Contains(t, page.Body.String(), "Cancel")
}

func TestUI_SchedulesPages(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")

	empty := env.do(t, http.MethodGet, "/ui/schedules", nil, cookies)
	require.Equal(t, http.StatusOK, empty.Code)
	assert.Contains(t, empty.Body.String(), "No schedules yet")

	sch, err := env.schedules.Create(adminUICtx(), app.CreateScheduleCommand{
		Name: "hourly-report", Cron: "@hourly", Queue: "default", JobType: "report", Payload: []byte(`{}`),
	})
	require.NoError(t, err)

	list := env.do(t, http.MethodGet, "/ui/schedules", nil, cookies)
	require.Equal(t, http.StatusOK, list.Code)
	assert.Contains(t, list.Body.String(), "hourly-report")
	assert.Contains(t, list.Body.String(), "Every hour")

	detail := env.do(t, http.MethodGet, "/ui/schedules/"+sch.ID.String(), nil, cookies)
	require.Equal(t, http.StatusOK, detail.Code)
	assert.Contains(t, detail.Body.String(), "Recent jobs")
	assert.Contains(t, detail.Body.String(), "Trigger now")
}

func TestUI_RetryDeadFragment(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")
	job, _, err := env.jobs.Enqueue(adminUICtx(), app.EnqueueCommand{
		Queue: "deadq", Type: "t", Payload: []byte(`{}`), MaxAttempts: intPtr(1),
	})
	require.NoError(t, err)
	leased, err := env.jobs.Lease(adminUICtx(), app.LeaseCommand{
		Queue: "deadq", WorkerID: "w1", MaxJobs: 1, LeaseDuration: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, leased, 1)
	_, err = env.jobs.Fail(adminUICtx(), app.FailCommand{
		JobID: leased[0].ID, WorkerID: "w1", Error: "boom", Permanent: true,
	})
	require.NoError(t, err)
	_ = job

	page := env.do(t, http.MethodGet, "/ui/queues/deadq?status=dead", nil, cookies)
	require.Equal(t, http.StatusOK, page.Code)
	assert.Contains(t, page.Body.String(), shortJob(leased[0].ID.String()))

	csrf := extractCSRF(t, page.Body.String())
	req := env.do(t, http.MethodPost, "/ui/queues/deadq/retry-dead", url.Values{"csrf": {csrf}}, cookies)
	// non-HTMX redirects
	require.Equal(t, http.StatusSeeOther, req.Code)
}

func TestUI_SSE_NoGoleak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/ui/events", nil)
	require.NoError(t, err)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := &sseRecorder{headers: make(http.Header), done: make(chan struct{})}
	go func() {
		env.handler.ServeHTTP(rec, req)
		close(rec.done)
	}()

	deadline := time.After(2 * time.Second)
	for !strings.Contains(rec.buf.String(), "event: overview") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for SSE event")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-rec.done:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE handler did not exit after cancel")
	}
}

func TestUI_SmokeParseHTML(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	cookies := env.login(t, "admin-secret")
	_, _, err := env.jobs.Enqueue(adminUICtx(), app.EnqueueCommand{Queue: "smoke", Type: "t", Payload: []byte(`{}`)})
	require.NoError(t, err)

	res := env.do(t, http.MethodGet, "/ui/", nil, cookies)
	require.Equal(t, http.StatusOK, res.Code)
	doc, err := html.Parse(strings.NewReader(res.Body.String()))
	require.NoError(t, err)
	found := false
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode && strings.Contains(n.Data, "smoke") {
			found = true
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	assert.True(t, found, "expected queue name in parsed HTML")
}

func (e *uiTestEnv) login(t *testing.T, secret string) []*http.Cookie {
	t.Helper()
	login := e.do(t, http.MethodGet, "/ui/login", nil, nil)
	require.Equal(t, http.StatusOK, login.Code)
	csrf := extractCSRF(t, login.Body.String())
	cookies := login.Result().Cookies()
	posted := e.do(t, http.MethodPost, "/ui/login", url.Values{
		"csrf": {csrf}, "api_key": {secret},
	}, cookies)
	require.Equal(t, http.StatusSeeOther, posted.Code)
	return mergeCookies(cookies, posted.Result().Cookies())
}

func adminUICtx() context.Context {
	return app.WithPrincipal(context.Background(), app.BootstrapAdmin)
}

func intPtr(n int) *int { return &n }

func shortJob(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

type sseRecorder struct {
	headers http.Header
	code    int
	buf     strings.Builder
	done    chan struct{}
}

func (r *sseRecorder) Header() http.Header { return r.headers }
func (r *sseRecorder) Write(b []byte) (int, error) {
	return r.buf.Write(b)
}
func (r *sseRecorder) WriteHeader(statusCode int) { r.code = statusCode }
func (r *sseRecorder) Flush()                     {}
