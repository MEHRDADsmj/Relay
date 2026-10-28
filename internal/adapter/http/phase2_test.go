package http_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestRelease_OKAndLeaseLost(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/release", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "pending", job["status"])

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/release", map[string]any{
		"worker_id": "other",
	}, nil)
	require.Equal(t, http.StatusConflict, res.Status)
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, "https://relay.dev/problems/lease-lost", problem["type"])
}

func TestCancel_PendingAndInvalidTransition(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/cancel", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "cancelled", job["status"])
	require.NotNil(t, job["cancelled_at"])

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/cancel", nil, nil)
	require.Equal(t, http.StatusConflict, res.Status)
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, "https://relay.dev/problems/invalid-transition", problem["type"])
}

func TestCancel_RunningSetsCancelRequested(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/cancel", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "running", job["status"])
	require.Equal(t, true, job["cancel_requested"])

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/heartbeat", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, true, job["cancel_requested"])
}

func TestRetry_DeadAndInvalid(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue":        "default",
		"type":         "email.send",
		"max_attempts": 1,
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/fail", map[string]any{
		"worker_id": "w1",
		"error":     "fatal",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "dead", job["status"])

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/retry", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "pending", job["status"])
	require.Equal(t, float64(0), job["attempts"])

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/retry", nil, nil)
	require.Equal(t, http.StatusConflict, res.Status)
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, "https://relay.dev/problems/invalid-transition", problem["type"])
}

func TestFail_Permanent(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue":        "default",
		"type":         "email.send",
		"max_attempts": 5,
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/fail", map[string]any{
		"worker_id": "w1",
		"error":     "no retry",
		"permanent": true,
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "dead", job["status"])
}

func TestListAttempts(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/complete", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodGet, "/v1/jobs/"+id+"/attempts", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var body map[string]any
	decodeBody(t, res, &body)
	attempts, ok := body["attempts"].([]any)
	require.True(t, ok)
	require.Len(t, attempts, 1)
	a := attempts[0].(map[string]any)
	require.Equal(t, "succeeded", a["outcome"])
	require.Equal(t, "w1", a["worker_id"])

	missing := uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000099"))
	res = doJSON(t, h, http.MethodGet, "/v1/jobs/"+missing.String()+"/attempts", nil, nil)
	require.Equal(t, http.StatusNotFound, res.Status)
}

func TestQueues_ListStatsPauseResume(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)

	res = doJSON(t, h, http.MethodGet, "/v1/queues", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var list map[string]any
	decodeBody(t, res, &list)
	queues, ok := list["queues"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, queues)

	res = doJSON(t, h, http.MethodGet, "/v1/queues/default", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var stats map[string]any
	decodeBody(t, res, &stats)
	require.Equal(t, "default", stats["queue"])
	require.Equal(t, false, stats["paused"])
	require.Equal(t, float64(1), stats["pending"])

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/pause", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var q map[string]any
	decodeBody(t, res, &q)
	require.Equal(t, true, q["paused"])

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var lease map[string]any
	decodeBody(t, res, &lease)
	require.Empty(t, lease["jobs"])

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/resume", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &q)
	require.Equal(t, false, q["paused"])

	res = doJSON(t, h, http.MethodGet, "/v1/queues/missingq", nil, nil)
	require.Equal(t, http.StatusNotFound, res.Status)
}

func TestListQueueJobs_PaginationAndInvalidCursor(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	ids := make([]domain.JobID, 3)
	for i := range ids {
		ids[i] = uuid.Must(uuid.NewV7())
	}
	h, _, _ := testRouterAt(t, now, ids...)

	for range 3 {
		res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
			"queue": "pageq",
			"type":  "t",
		}, nil)
		require.Equal(t, http.StatusCreated, res.Status)
	}

	res := doJSON(t, h, http.MethodGet, "/v1/queues/pageq/jobs?status=pending&limit=2", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var page map[string]any
	decodeBody(t, res, &page)
	jobs, ok := page["jobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs, 2)
	next, ok := page["next_cursor"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	res = doJSON(t, h, http.MethodGet, "/v1/queues/pageq/jobs?status=pending&limit=2&cursor="+next, nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &page)
	jobs = page["jobs"].([]any)
	require.Len(t, jobs, 1)
	require.Equal(t, "", page["next_cursor"])

	res = doJSON(t, h, http.MethodGet, "/v1/queues/pageq/jobs?status=pending&cursor=!!!", nil, nil)
	require.Equal(t, http.StatusBadRequest, res.Status)
	var problem map[string]any
	decodeBody(t, res, &problem)
	errs, ok := problem["errors"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, errs)
	fe := errs[0].(map[string]any)
	require.Equal(t, "cursor", fe["field"])
}

func TestRetryDeadJobs(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue":        "default",
		"type":         "email.send",
		"max_attempts": 1,
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	id := job["id"].(string)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/fail", map[string]any{
		"worker_id": "w1",
		"error":     "fatal",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/dead/retry", map[string]any{
		"limit": 10,
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var body map[string]any
	decodeBody(t, res, &body)
	require.Equal(t, float64(1), body["retried"])

	res = doJSON(t, h, http.MethodGet, "/v1/jobs/"+id, nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "pending", job["status"])

	res = doJSON(t, h, http.MethodPost, "/v1/queues/nosuch/dead/retry", map[string]any{}, nil)
	require.Equal(t, http.StatusNotFound, res.Status)
}

func TestLease_LongPollTimeout(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	wait := 2
	start := time.Now()
	res := doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id":    "w1",
		"wait_seconds": wait,
	}, nil)
	elapsed := time.Since(start)
	require.Equal(t, http.StatusOK, res.Status)
	var lease map[string]any
	decodeBody(t, res, &lease)
	require.Empty(t, lease["jobs"])
	require.GreaterOrEqual(t, elapsed, time.Duration(wait)*time.Second)
	require.Less(t, elapsed, time.Duration(wait)*time.Second+time.Second)
}

func TestLease_LongPollClientDisconnect(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	ctx, cancel := context.WithCancel(context.Background())
	body := strings.NewReader(`{"worker_id":"w1","wait_seconds":30}`)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/queues/default/leases", body)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	var wg sync.WaitGroup
	wg.Add(1)
	start := time.Now()
	go func() {
		defer wg.Done()
		h.ServeHTTP(rec, req)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
	elapsed := time.Since(start)
	require.Less(t, elapsed, 2*time.Second, "handler should return promptly on client disconnect")
	require.Equal(t, http.StatusOK, rec.Code, "disconnect must not be 500")
	var lease map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lease))
	jobs, ok := lease["jobs"].([]any)
	require.True(t, ok)
	require.Empty(t, jobs)
}

func TestLease_LongPollBaseContextCancel(t *testing.T) {
	// Mirrors cmd/relay-server BaseContext: cancelling the serve ctx must unblock
	// in-flight LeaseWait so Shutdown does not wait out wait_seconds.
	t.Parallel()
	h, _ := testRouter(t, "")

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{
		Handler: h,
		BaseContext: func(_ net.Listener) context.Context {
			return baseCtx
		},
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      45 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	t.Cleanup(func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
		<-serveErr
	})

	type result struct {
		status  int
		body    []byte
		elapsed time.Duration
		err     error
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"http://"+ln.Addr().String()+"/v1/queues/default/leases",
			strings.NewReader(`{"worker_id":"w1","wait_seconds":30}`))
		if err != nil {
			done <- result{err: err, elapsed: time.Since(start)}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		elapsed := time.Since(start)
		if err != nil {
			done <- result{err: err, elapsed: elapsed}
			return
		}
		defer func() { _ = res.Body.Close() }()
		body, _ := io.ReadAll(res.Body)
		done <- result{status: res.StatusCode, body: body, elapsed: elapsed}
	}()

	time.Sleep(200 * time.Millisecond) // ensure long-poll is blocked in LeaseWait
	cancelBase()

	got := <-done
	require.NoError(t, got.err)
	require.Less(t, got.elapsed, 2*time.Second, "long-poll must return promptly when BaseContext cancels")
	require.Equal(t, http.StatusOK, got.status)
	var lease map[string]any
	require.NoError(t, json.Unmarshal(got.body, &lease))
	jobs, ok := lease["jobs"].([]any)
	require.True(t, ok)
	require.Empty(t, jobs)
}

func TestLease_WaitSecondsValidation(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id":    "w1",
		"wait_seconds": 31,
	}, nil)
	require.Equal(t, http.StatusBadRequest, res.Status)
}

func TestRelease_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	missing := uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000099"))
	res := doJSON(t, h, http.MethodPost, "/v1/jobs/"+missing.String()+"/release", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusNotFound, res.Status)
}

func TestCancel_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	missing := uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000099"))
	res := doJSON(t, h, http.MethodPost, "/v1/jobs/"+missing.String()+"/cancel", nil, nil)
	require.Equal(t, http.StatusNotFound, res.Status)
}

func TestJobIncludesCancelRequestedFalse(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	require.Equal(t, false, job["cancel_requested"])
	// Ensure JSON marshals the bool (required field).
	raw := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(res.Body, &raw))
	require.Contains(t, raw, "cancel_requested")
}
