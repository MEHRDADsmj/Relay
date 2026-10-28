package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/pkg/sdk"
)

func TestClient_EnqueueAndGetJob(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{apiKey: "secret"}
	var gotBody map[string]any
	api.enqueueFn = func(_ *http.Request, body map[string]any) (int, map[string]any) {
		gotBody = body
		return http.StatusCreated, sampleJob("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", body)
	}
	api.getJobFn = func(id string) (int, map[string]any) {
		require.Equal(t, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", id)
		j := sampleJob(id, map[string]any{"queue": "q1", "type": "email"})
		j["status"] = "succeeded"
		return http.StatusOK, j
	}
	srv := api.start(t)
	c := sdk.NewClient(srv.URL, sdk.WithAPIKey("secret"))

	info, err := c.Enqueue(context.Background(), sdk.EnqueueRequest{
		Queue:          "q1",
		Type:           "email",
		Payload:        map[string]any{"to": "a@b.c"},
		Priority:       3,
		MaxAttempts:    7,
		IdempotencyKey: "idem-1",
		RunAt:          time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	require.NoError(t, err)
	require.Equal(t, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", info.ID)
	require.Equal(t, "q1", gotBody["queue"])
	require.Equal(t, "email", gotBody["type"])
	require.EqualValues(t, 3, gotBody["priority"])
	require.EqualValues(t, 7, gotBody["max_attempts"])
	require.Equal(t, "idem-1", gotBody["idempotency_key"])
	require.NotNil(t, gotBody["payload"])

	got, err := c.GetJob(context.Background(), info.ID)
	require.NoError(t, err)
	require.Equal(t, "succeeded", got.Status)
}

func TestClient_APIError_NotFoundAndLeaseLost(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs/missing", func(w http.ResponseWriter, _ *http.Request) {
		writeProblem(w, http.StatusNotFound, "Not Found", "resource not found", "")
	})
	mux.HandleFunc("/v1/jobs/lost", func(w http.ResponseWriter, _ *http.Request) {
		writeProblem(w, http.StatusConflict, "Conflict", "lease lost or conflict", "https://relay.dev/problems/lease-lost")
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c := sdk.NewClient(srv.URL)

	_, err := c.GetJob(context.Background(), "missing")
	require.Error(t, err)
	require.True(t, errors.Is(err, sdk.ErrNotFound))
	var ae *sdk.APIError
	require.True(t, errors.As(err, &ae))
	require.Equal(t, 404, ae.StatusCode)

	_, err = c.GetJob(context.Background(), "lost")
	require.True(t, errors.Is(err, sdk.ErrLeaseLost))
}

func TestClient_Unauthorized(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{apiKey: "secret"}
	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	_, err := c.Enqueue(context.Background(), sdk.EnqueueRequest{Queue: "q", Type: "t"})
	require.Error(t, err)
	var ae *sdk.APIError
	require.True(t, errors.As(err, &ae))
	require.Equal(t, 401, ae.StatusCode)
}

func TestClient_PayloadMarshaling(t *testing.T) {
	t.Parallel()
	api := &fakeAPI{}
	api.enqueueFn = func(_ *http.Request, body map[string]any) (int, map[string]any) {
		raw, _ := json.Marshal(body["payload"])
		require.JSONEq(t, `{"N":1}`, string(raw))
		return http.StatusCreated, sampleJob("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", body)
	}
	srv := api.start(t)
	c := sdk.NewClient(srv.URL)
	_, err := c.Enqueue(context.Background(), sdk.EnqueueRequest{
		Queue:   "q",
		Type:    "t",
		Payload: struct{ N int }{N: 1},
	})
	require.NoError(t, err)
}
