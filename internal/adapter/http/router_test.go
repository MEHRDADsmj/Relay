package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	relayhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type seqIDs struct {
	ids []domain.JobID
	i   int
}

func (s *seqIDs) NewID() domain.JobID {
	if s.i >= len(s.ids) {
		return uuid.Must(uuid.NewV7())
	}
	id := s.ids[s.i]
	s.i++
	return id
}

type httpResult struct {
	Status int
	Header http.Header
	Body   []byte
}

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func phase3Services(repo *memory.Repository, clock fixedClock, jobs *app.JobService) (*app.QueueService, *app.ScheduleService, *app.APIKeyService) {
	queues := app.NewQueueService(repo, repo, repo, clock)
	schedules := app.NewScheduleService(repo, jobs, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	return queues, schedules, apiKeys
}

func mountRouter(
	repo *memory.Repository,
	clock fixedClock,
	jobs *app.JobService,
	queues *app.QueueService,
	schedules *app.ScheduleService,
	apiKeys *app.APIKeyService,
	opts relayhttp.Options,
) http.Handler {
	if opts.Logger == nil {
		opts.Logger = discardLog()
	}
	if opts.Auth == nil {
		opts.Auth = app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: opts.APIKey}, discardLog())
	}
	return relayhttp.NewRouter(jobs, queues, schedules, apiKeys, opts)
}

func testRouter(t *testing.T, apiKey string) (http.Handler, *app.JobService) {
	t.Helper()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	id1 := uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000001"))
	id2 := uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000002"))
	repo := memory.New()
	clock := fixedClock{t: now}
	svc := app.NewJobService(
		repo,
		clock,
		&seqIDs{ids: []domain.JobID{id1, id2}},
		app.FixedDelayRetryPolicy{Delay: time.Second},
		discardLog(),
		repo, // memory notifier for long-poll
	)
	queues, schedules, apiKeys := phase3Services(repo, clock, svc)
	h := mountRouter(repo, clock, svc, queues, schedules, apiKeys, relayhttp.Options{APIKey: apiKey})
	return h, svc
}

func testRouterAt(t *testing.T, now time.Time, ids ...domain.JobID) (http.Handler, *app.JobService, *memory.Repository) {
	t.Helper()
	repo := memory.New()
	clock := fixedClock{t: now}
	svc := app.NewJobService(
		repo,
		clock,
		&seqIDs{ids: ids},
		app.FixedDelayRetryPolicy{Delay: time.Second},
		discardLog(),
		repo,
	)
	queues, schedules, apiKeys := phase3Services(repo, clock, svc)
	h := mountRouter(repo, clock, svc, queues, schedules, apiKeys, relayhttp.Options{})
	return h, svc, repo
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any, headers map[string]string) httpResult {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := rec.Result()
	defer func() { require.NoError(t, res.Body.Close()) }()
	raw, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return httpResult{Status: res.StatusCode, Header: res.Header.Clone(), Body: raw}
}

func decodeBody(t *testing.T, res httpResult, dst any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(res.Body, dst))
}

func TestHealthz(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodGet, "/healthz", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var body map[string]string
	decodeBody(t, res, &body)
	require.Equal(t, "ok", body["status"])
	require.NotEmpty(t, res.Header.Get("X-Request-ID"))
}

func TestReadyz_OK(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodGet, "/readyz", nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var body map[string]string
	decodeBody(t, res, &body)
	require.Equal(t, "ok", body["status"])
}

func TestEnqueueGetLeaseHeartbeatCompleteFail(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue":   "default",
		"type":    "email.send",
		"payload": map[string]any{"to": "a@b.c"},
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	require.Empty(t, res.Header.Get("Idempotent-Replayed"))
	var job map[string]any
	decodeBody(t, res, &job)
	id, ok := job["id"].(string)
	require.True(t, ok)
	require.Equal(t, "pending", job["status"])
	require.Equal(t, "default", job["queue"])

	res = doJSON(t, h, http.MethodGet, "/v1/jobs/"+id, nil, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, id, job["id"])

	res = doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
		"max_jobs":  1,
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var lease map[string]any
	decodeBody(t, res, &lease)
	jobs, ok := lease["jobs"].([]any)
	require.True(t, ok)
	require.Len(t, jobs, 1)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/heartbeat", map[string]any{
		"worker_id":     "w1",
		"lease_seconds": 60,
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs/"+id+"/complete", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "succeeded", job["status"])
}

func TestFail_RetriesToPending(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue":        "default",
		"type":         "email.send",
		"max_attempts": 3,
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
		"error":     "boom",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &job)
	require.Equal(t, "pending", job["status"])
	require.Equal(t, "boom", job["last_error"])
}

func TestIdempotentReplay_Header(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	body := map[string]any{
		"queue":           "default",
		"type":            "email.send",
		"idempotency_key": "ik-1",
	}
	res := doJSON(t, h, http.MethodPost, "/v1/jobs", body, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var first map[string]any
	decodeBody(t, res, &first)

	res = doJSON(t, h, http.MethodPost, "/v1/jobs", body, nil)
	require.Equal(t, http.StatusOK, res.Status)
	require.Equal(t, "true", res.Header.Get("Idempotent-Replayed"))
	var second map[string]any
	decodeBody(t, res, &second)
	require.Equal(t, first["id"], second["id"])
}

func TestLease_Empty(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id": "w1",
	}, nil)
	require.Equal(t, http.StatusOK, res.Status)
	var lease map[string]any
	decodeBody(t, res, &lease)
	jobs, ok := lease["jobs"].([]any)
	require.True(t, ok)
	require.Empty(t, jobs)
}

func TestErrors_ValidationNotFoundLeaseLost(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "BAD QUEUE",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusBadRequest, res.Status)
	require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, float64(400), problem["status"])
	errs, ok := problem["errors"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, errs)

	missing := uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000099"))
	res = doJSON(t, h, http.MethodGet, "/v1/jobs/"+missing.String(), nil, nil)
	require.Equal(t, http.StatusNotFound, res.Status)
	decodeBody(t, res, &problem)
	require.Equal(t, float64(404), problem["status"])

	res = doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
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
		"worker_id": "other",
	}, nil)
	require.Equal(t, http.StatusConflict, res.Status)
	decodeBody(t, res, &problem)
	require.Equal(t, float64(409), problem["status"])
	require.Equal(t, "https://relay.dev/problems/lease-lost", problem["type"])
}

func TestErrors_InvalidJSONAndBadUUID(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/jobs", strings.NewReader("{not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))

	req = httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/jobs/not-a-uuid", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestErrors_BodyTooLarge(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := fixedClock{t: now}
	svc := app.NewJobService(repo, clock, &seqIDs{}, app.FixedDelayRetryPolicy{Delay: time.Second}, nil, nil)
	queues, schedules, apiKeys := phase3Services(repo, clock, svc)
	h := mountRouter(repo, clock, svc, queues, schedules, apiKeys, relayhttp.Options{
		MaxBodyBytes: 64,
	})
	big := strings.Repeat("a", 128)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/jobs", strings.NewReader(`{"queue":"default","type":"t","payload":{"x":"`+big+`"}}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	require.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
}

func TestAuth_OnAndOff(t *testing.T) {
	t.Parallel()

	t.Run("off", func(t *testing.T) {
		t.Parallel()
		h, _ := testRouter(t, "")
		res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
			"queue": "default",
			"type":  "email.send",
		}, nil)
		require.Equal(t, http.StatusCreated, res.Status)
	})

	t.Run("on_rejects", func(t *testing.T) {
		t.Parallel()
		h, _ := testRouter(t, "secret-key")
		res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
			"queue": "default",
			"type":  "email.send",
		}, nil)
		require.Equal(t, http.StatusUnauthorized, res.Status)
		require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))

		res = doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
			"queue": "default",
			"type":  "email.send",
		}, map[string]string{"Authorization": "Bearer wrong"})
		require.Equal(t, http.StatusUnauthorized, res.Status)
	})

	t.Run("on_accepts", func(t *testing.T) {
		t.Parallel()
		h, _ := testRouter(t, "secret-key")
		res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
			"queue": "default",
			"type":  "email.send",
		}, map[string]string{"Authorization": "Bearer secret-key"})
		require.Equal(t, http.StatusCreated, res.Status)
	})

	t.Run("health_exempt", func(t *testing.T) {
		t.Parallel()
		h, _ := testRouter(t, "secret-key")
		res := doJSON(t, h, http.MethodGet, "/healthz", nil, nil)
		require.Equal(t, http.StatusOK, res.Status)
		res = doJSON(t, h, http.MethodGet, "/readyz", nil, nil)
		require.Equal(t, http.StatusOK, res.Status)
	})
}

func TestRequestID_PropagatesClientValue(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodGet, "/healthz", nil, map[string]string{
		"X-Request-ID": "client-rid-1",
	})
	require.Equal(t, http.StatusOK, res.Status)
	require.Equal(t, "client-rid-1", res.Header.Get("X-Request-ID"))
}

func TestReadyz_Unavailable(t *testing.T) {
	t.Parallel()
	clock := fixedClock{t: time.Now().UTC()}
	repo := memory.New()
	svc := app.NewJobService(
		failingPingRepo{},
		clock,
		&seqIDs{},
		app.FixedDelayRetryPolicy{Delay: time.Second},
		nil,
		nil,
	)
	queues, schedules, apiKeys := phase3Services(repo, clock, svc)
	h := mountRouter(repo, clock, svc, queues, schedules, apiKeys, relayhttp.Options{})
	res := doJSON(t, h, http.MethodGet, "/readyz", nil, nil)
	require.Equal(t, http.StatusServiceUnavailable, res.Status)
	require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
}

func TestErrors_InternalServerError(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	clock := fixedClock{t: now}
	mem := memory.New()
	svc := app.NewJobService(
		errorCreateRepo{err: errBoom},
		clock,
		&seqIDs{ids: []domain.JobID{uuid.Must(uuid.Parse("01900000-0000-7000-8000-000000000010"))}},
		app.FixedDelayRetryPolicy{Delay: time.Second},
		discardLog(),
		nil,
	)
	queues, schedules, apiKeys := phase3Services(mem, clock, svc)
	h := mountRouter(mem, clock, svc, queues, schedules, apiKeys, relayhttp.Options{})
	res := doJSON(t, h, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": "default",
		"type":  "email.send",
	}, nil)
	require.Equal(t, http.StatusInternalServerError, res.Status)
	require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, float64(500), problem["status"])
	require.Equal(t, "Internal Server Error", problem["title"])
	require.Equal(t, "an unexpected error occurred", problem["detail"])
	require.NotContains(t, problem, "errors")
}

func TestErrors_LeaseSecondsFieldRemap(t *testing.T) {
	t.Parallel()
	h, _ := testRouter(t, "")
	res := doJSON(t, h, http.MethodPost, "/v1/queues/default/leases", map[string]any{
		"worker_id":     "w1",
		"lease_seconds": 1, // below domain MinLeaseDuration (5s) ? lease_duration validation
	}, nil)
	require.Equal(t, http.StatusBadRequest, res.Status)
	require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, float64(400), problem["status"])
	errs, ok := problem["errors"].([]any)
	require.True(t, ok)
	require.Len(t, errs, 1)
	fe, ok := errs[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "lease_seconds", fe["field"])
	require.NotEqual(t, "lease_duration", fe["field"])
	require.NotEmpty(t, fe["message"])
}

func TestFail_ExhaustsToDead(t *testing.T) {
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
	require.Equal(t, "fatal", job["last_error"])
}

var errBoom = errors.New("storage boom")

// failingPingRepo is a minimal JobRepository that fails Ping.
type failingPingRepo struct{}

func (failingPingRepo) Create(context.Context, domain.Job) (domain.Job, bool, error) {
	return domain.Job{}, false, nil
}

func (failingPingRepo) Get(context.Context, domain.JobID) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) Claim(context.Context, string, string, int, time.Duration, time.Time) ([]domain.Job, error) {
	return nil, nil
}

func (failingPingRepo) ExtendLease(context.Context, domain.JobID, string, time.Duration, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) Complete(context.Context, domain.JobID, string, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) Fail(context.Context, domain.JobID, string, string, domain.FailureDecision, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) Release(context.Context, domain.JobID, string, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) Cancel(context.Context, domain.JobID, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) Retry(context.Context, domain.JobID, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (failingPingRepo) ListAttempts(context.Context, domain.JobID) ([]domain.Attempt, error) {
	return nil, domain.ErrNotFound
}

func (failingPingRepo) ListByStatus(context.Context, string, domain.Status, domain.PageRequest) (domain.Page[domain.Job], error) {
	return domain.Page[domain.Job]{}, domain.ErrNotFound
}

func (failingPingRepo) ListJobsBySchedule(context.Context, domain.ScheduleID, int) ([]domain.Job, error) {
	return nil, domain.ErrNotFound
}

func (failingPingRepo) Ping(context.Context) error {
	return context.DeadlineExceeded
}

// errorCreateRepo returns err from Create so the HTTP layer maps it to 500.
type errorCreateRepo struct {
	err error
}

func (r errorCreateRepo) Create(context.Context, domain.Job) (domain.Job, bool, error) {
	return domain.Job{}, false, r.err
}

func (errorCreateRepo) Get(context.Context, domain.JobID) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) Claim(context.Context, string, string, int, time.Duration, time.Time) ([]domain.Job, error) {
	return nil, nil
}

func (errorCreateRepo) ExtendLease(context.Context, domain.JobID, string, time.Duration, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) Complete(context.Context, domain.JobID, string, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) Fail(context.Context, domain.JobID, string, string, domain.FailureDecision, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) Release(context.Context, domain.JobID, string, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) Cancel(context.Context, domain.JobID, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) Retry(context.Context, domain.JobID, time.Time) (domain.Job, error) {
	return domain.Job{}, domain.ErrNotFound
}

func (errorCreateRepo) ListAttempts(context.Context, domain.JobID) ([]domain.Attempt, error) {
	return nil, domain.ErrNotFound
}

func (errorCreateRepo) ListByStatus(context.Context, string, domain.Status, domain.PageRequest) (domain.Page[domain.Job], error) {
	return domain.Page[domain.Job]{}, domain.ErrNotFound
}

func (errorCreateRepo) ListJobsBySchedule(context.Context, domain.ScheduleID, int) ([]domain.Job, error) {
	return nil, domain.ErrNotFound
}

func (errorCreateRepo) Ping(context.Context) error { return nil }
