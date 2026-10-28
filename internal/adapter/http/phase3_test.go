package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	relayhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
)

type phase3Stack struct {
	h         http.Handler
	repo      *memory.Repository
	clock     fixedClock
	jobs      *app.JobService
	schedules *app.ScheduleService
	apiKeys   *app.APIKeyService
	auth      *app.AuthService
}

func newPhase3Stack(t *testing.T, bootstrap string) *phase3Stack {
	t.Helper()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := fixedClock{t: now}
	log := discardLog()
	jobs := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, log, repo)
	queues := app.NewQueueService(repo, repo, repo, clock)
	schedules := app.NewScheduleService(repo, jobs, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: bootstrap}, log)
	h := relayhttp.NewRouter(jobs, queues, schedules, apiKeys, relayhttp.Options{
		APIKey: bootstrap,
		Auth:   auth,
		Logger: log,
	})
	return &phase3Stack{h: h, repo: repo, clock: clock, jobs: jobs, schedules: schedules, apiKeys: apiKeys, auth: auth}
}

func (s *phase3Stack) adminHeaders() map[string]string {
	return map[string]string{"Authorization": "Bearer " + "bootstrap-admin"}
}

func TestPhase3_SchedulesHappyPath(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "bootstrap-admin")
	hdr := s.adminHeaders()

	res := doJSON(t, s.h, http.MethodPost, "/v1/schedules", map[string]any{
		"name":  "hourly-mail",
		"cron":  "@hourly",
		"queue": "default",
		"type":  "email.send",
		"payload": map[string]any{
			"to": "a@b.c",
		},
	}, hdr)
	require.Equal(t, http.StatusCreated, res.Status)
	var sched map[string]any
	decodeBody(t, res, &sched)
	id := sched["id"].(string)
	updatedAt := sched["updated_at"].(string)
	require.Equal(t, "hourly-mail", sched["name"])
	require.Equal(t, "@hourly", sched["cron"])

	res = doJSON(t, s.h, http.MethodGet, "/v1/schedules/"+id, nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, s.h, http.MethodGet, "/v1/schedules/hourly-mail", nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)

	res = doJSON(t, s.h, http.MethodGet, "/v1/schedules", nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)
	var page map[string]any
	decodeBody(t, res, &page)
	require.Len(t, page["schedules"].([]any), 1)

	res = doJSON(t, s.h, http.MethodPatch, "/v1/schedules/"+id, map[string]any{
		"cron": "@daily",
	}, map[string]string{
		"Authorization": hdr["Authorization"],
		"If-Match":      updatedAt,
	})
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &sched)
	require.Equal(t, "@daily", sched["cron"])

	res = doJSON(t, s.h, http.MethodPost, "/v1/schedules/"+id+"/disable", nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &sched)
	require.Equal(t, false, sched["enabled"])

	res = doJSON(t, s.h, http.MethodPost, "/v1/schedules/"+id+"/enable", nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)
	decodeBody(t, res, &sched)
	require.Equal(t, true, sched["enabled"])

	res = doJSON(t, s.h, http.MethodPost, "/v1/schedules/"+id+"/trigger", nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)
	var job map[string]any
	decodeBody(t, res, &job)
	require.Equal(t, "email.send", job["type"])
	require.Equal(t, "default", job["queue"])

	res = doJSON(t, s.h, http.MethodDelete, "/v1/schedules/"+id, nil, hdr)
	require.Equal(t, http.StatusNoContent, res.Status)

	res = doJSON(t, s.h, http.MethodGet, "/v1/schedules/"+id, nil, hdr)
	require.Equal(t, http.StatusNotFound, res.Status)
}

func TestPhase3_APIKeysHappyPath(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "bootstrap-admin")
	hdr := s.adminHeaders()

	res := doJSON(t, s.h, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "producer-1",
		"scopes": []string{"producer"},
	}, hdr)
	require.Equal(t, http.StatusCreated, res.Status)
	var created map[string]any
	decodeBody(t, res, &created)
	secret := created["secret"].(string)
	require.NotEmpty(t, secret)
	key := created["key"].(map[string]any)
	id := key["id"].(string)
	require.Equal(t, "producer-1", key["name"])

	res = doJSON(t, s.h, http.MethodGet, "/v1/api-keys", nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)
	var list map[string]any
	decodeBody(t, res, &list)
	require.Len(t, list["keys"].([]any), 1)
	listed := list["keys"].([]any)[0].(map[string]any)
	_, hasSecret := listed["secret"]
	require.False(t, hasSecret)

	res = doJSON(t, s.h, http.MethodDelete, "/v1/api-keys/"+id, nil, hdr)
	require.Equal(t, http.StatusOK, res.Status)
	var revoked map[string]any
	decodeBody(t, res, &revoked)
	require.NotNil(t, revoked["revoked_at"])

	// Revoked secret must not authenticate.
	res = doJSON(t, s.h, http.MethodGet, "/v1/api-keys", nil, map[string]string{
		"Authorization": "Bearer " + secret,
	})
	require.Equal(t, http.StatusUnauthorized, res.Status)
}

func TestPhase3_Authz_401And403(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "bootstrap-admin")
	admin := s.adminHeaders()

	res := doJSON(t, s.h, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "prod",
		"scopes": []string{"producer"},
	}, admin)
	require.Equal(t, http.StatusCreated, res.Status)
	var created map[string]any
	decodeBody(t, res, &created)
	producerSecret := created["secret"].(string)

	t.Run("401_missing", func(t *testing.T) {
		t.Parallel()
		res := doJSON(t, s.h, http.MethodGet, "/v1/schedules", nil, nil)
		require.Equal(t, http.StatusUnauthorized, res.Status)
		require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
	})

	t.Run("401_invalid", func(t *testing.T) {
		t.Parallel()
		res := doJSON(t, s.h, http.MethodGet, "/v1/schedules", nil, map[string]string{
			"Authorization": "Bearer totally-wrong",
		})
		require.Equal(t, http.StatusUnauthorized, res.Status)
	})

	t.Run("403_producer_cannot_admin", func(t *testing.T) {
		t.Parallel()
		res := doJSON(t, s.h, http.MethodGet, "/v1/schedules", nil, map[string]string{
			"Authorization": "Bearer " + producerSecret,
		})
		require.Equal(t, http.StatusForbidden, res.Status)
		require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
		var problem map[string]any
		decodeBody(t, res, &problem)
		require.Equal(t, float64(403), problem["status"])
		require.Equal(t, "Forbidden", problem["title"])
	})

	t.Run("producer_can_enqueue", func(t *testing.T) {
		t.Parallel()
		res := doJSON(t, s.h, http.MethodPost, "/v1/jobs", map[string]any{
			"queue": "default",
			"type":  "email.send",
		}, map[string]string{
			"Authorization": "Bearer " + producerSecret,
		})
		require.Equal(t, http.StatusCreated, res.Status)
	})
}

func TestPhase3_Schedule_IfMatch_428And412(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "bootstrap-admin")
	hdr := s.adminHeaders()

	res := doJSON(t, s.h, http.MethodPost, "/v1/schedules", map[string]any{
		"name":  "patch-me",
		"cron":  "@hourly",
		"queue": "default",
		"type":  "email.send",
	}, hdr)
	require.Equal(t, http.StatusCreated, res.Status)
	var sched map[string]any
	decodeBody(t, res, &sched)
	id := sched["id"].(string)
	updatedAt := sched["updated_at"].(string)

	res = doJSON(t, s.h, http.MethodPatch, "/v1/schedules/"+id, map[string]any{
		"cron": "@daily",
	}, hdr)
	require.Equal(t, http.StatusPreconditionRequired, res.Status)
	require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
	var problem map[string]any
	decodeBody(t, res, &problem)
	require.Equal(t, float64(428), problem["status"])
	require.Equal(t, "Precondition Required", problem["title"])

	stale := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	res = doJSON(t, s.h, http.MethodPatch, "/v1/schedules/"+id, map[string]any{
		"cron": "@daily",
	}, map[string]string{
		"Authorization": hdr["Authorization"],
		"If-Match":      stale,
	})
	require.Equal(t, http.StatusPreconditionFailed, res.Status)
	require.Equal(t, "application/problem+json", res.Header.Get("Content-Type"))
	decodeBody(t, res, &problem)
	require.Equal(t, float64(412), problem["status"])
	require.Equal(t, "Precondition Failed", problem["title"])

	res = doJSON(t, s.h, http.MethodPatch, "/v1/schedules/"+id, map[string]any{
		"cron": "@daily",
	}, map[string]string{
		"Authorization": hdr["Authorization"],
		"If-Match":      updatedAt,
	})
	require.Equal(t, http.StatusOK, res.Status, string(res.Body))
}

func TestPhase3_AuthService_StoredKeyBootstrap(t *testing.T) {
	t.Parallel()
	// Empty bootstrap: auth open until a key exists; after create via open admin, auth required.
	s := newPhase3Stack(t, "")
	res := doJSON(t, s.h, http.MethodPost, "/v1/api-keys", map[string]any{
		"name":   "admin",
		"scopes": []string{"admin"},
	}, nil)
	require.Equal(t, http.StatusCreated, res.Status)
	var created map[string]any
	decodeBody(t, res, &created)
	secret := created["secret"].(string)

	res = doJSON(t, s.h, http.MethodGet, "/v1/api-keys", nil, nil)
	require.Equal(t, http.StatusUnauthorized, res.Status)

	res = doJSON(t, s.h, http.MethodGet, "/v1/api-keys", nil, map[string]string{
		"Authorization": "Bearer " + secret,
	})
	require.Equal(t, http.StatusOK, res.Status)
}

func TestPhase3_CreateSchedule_Conflict(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "bootstrap-admin")
	hdr := s.adminHeaders()
	body := map[string]any{
		"name":  "dup",
		"cron":  "@hourly",
		"queue": "default",
		"type":  "email.send",
	}
	res := doJSON(t, s.h, http.MethodPost, "/v1/schedules", body, hdr)
	require.Equal(t, http.StatusCreated, res.Status)
	res = doJSON(t, s.h, http.MethodPost, "/v1/schedules", body, hdr)
	require.Equal(t, http.StatusConflict, res.Status)
}

func TestPhase3_IfMatch_QuotedETag(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "bootstrap-admin")
	hdr := s.adminHeaders()
	res := doJSON(t, s.h, http.MethodPost, "/v1/schedules", map[string]any{
		"name":  "quoted-etag",
		"cron":  "@hourly",
		"queue": "default",
		"type":  "email.send",
	}, hdr)
	require.Equal(t, http.StatusCreated, res.Status)
	var sched map[string]any
	decodeBody(t, res, &sched)
	id := sched["id"].(string)
	updatedAt := sched["updated_at"].(string)

	res = doJSON(t, s.h, http.MethodPatch, "/v1/schedules/"+id, map[string]any{
		"enabled": false,
	}, map[string]string{
		"Authorization": hdr["Authorization"],
		"If-Match":      `"` + updatedAt + `"`,
	})
	require.Equal(t, http.StatusOK, res.Status, string(res.Body))
}

func TestPhase3_ProblemJSONShape(t *testing.T) {
	t.Parallel()
	s := newPhase3Stack(t, "secret")
	res := doJSON(t, s.h, http.MethodGet, "/v1/schedules", nil, nil)
	require.Equal(t, http.StatusUnauthorized, res.Status)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(res.Body, &raw))
	require.Contains(t, raw, "type")
	require.Contains(t, raw, "title")
	require.Contains(t, raw, "status")
	_ = context.Background()
}
