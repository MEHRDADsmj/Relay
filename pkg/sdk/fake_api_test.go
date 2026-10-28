package sdk_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// fakeAPI is a minimal Relay HTTP surface for SDK tests.
type fakeAPI struct {
	mu sync.Mutex

	apiKey       string
	conflictType string // problem type URI for 409 responses

	enqueueFn    func(r *http.Request, body map[string]any) (status int, job map[string]any)
	getJobFn     func(id string) (status int, job map[string]any)
	leaseFn      func(queue string, body map[string]any) (status int, jobs []map[string]any)
	heartbeatFn  func(id string, body map[string]any) (status int, job map[string]any)
	completeFn   func(id string, body map[string]any) int
	failFn       func(id string, body map[string]any) int
	releaseFn    func(id string, body map[string]any) int
	cancelFn     func(id string) (status int, job map[string]any)
	retryFn      func(id string) (status int, job map[string]any)
	attemptsFn   func(id string) (status int, attempts []map[string]any)
	listQueuesFn func() (status int, queues []map[string]any)
	queueStatsFn func(queue string) (status int, stats map[string]any)
	pauseFn      func(queue string) (status int, info map[string]any)
	resumeFn     func(queue string) (status int, info map[string]any)
	listJobsFn   func(queue, jobStatus, cursor string, limit int) (status int, jobs []map[string]any, nextCursor string)
	retryDeadFn  func(queue string, body map[string]any) (status int, retried int)

	leaseCalls     atomic.Int32
	heartbeatCalls atomic.Int32
	completeCalls  atomic.Int32
	failCalls      atomic.Int32
	releaseCalls   atomic.Int32
	failErrors     []string
	maxInFlight    atomic.Int32
	inFlight       atomic.Int32
}

func (f *fakeAPI) start(t testingT) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/jobs", f.handleJobs)
	mux.HandleFunc("/v1/jobs/", f.handleJobSub)
	mux.HandleFunc("/v1/queues", f.handleQueuesRoot)
	mux.HandleFunc("/v1/queues/", f.handleQueues)
	srv := httptest.NewServer(f.auth(mux))
	t.Cleanup(srv.Close)
	return srv
}

type testingT interface {
	Helper()
	Cleanup(func())
}

func (f *fakeAPI) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.apiKey != "" {
			want := "Bearer " + f.apiKey
			if r.Header.Get("Authorization") != want {
				writeProblem(w, http.StatusUnauthorized, "Unauthorized", "missing or invalid API key", "")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (f *fakeAPI) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	body := readJSON(r)
	if f.enqueueFn == nil {
		writeJSON(w, http.StatusCreated, sampleJob("11111111-1111-1111-1111-111111111111", body))
		return
	}
	status, job := f.enqueueFn(r, body)
	writeJSON(w, status, job)
}

func (f *fakeAPI) handleJobSub(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/jobs/")
	parts := strings.Split(path, "/")
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		if f.getJobFn == nil {
			writeProblem(w, http.StatusNotFound, "Not Found", "resource not found", "")
			return
		}
		status, job := f.getJobFn(id)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "resource not found", "")
			return
		}
		writeJSON(w, status, job)
		return
	}
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	action := parts[1]
	switch {
	case action == "attempts" && r.Method == http.MethodGet:
		if f.attemptsFn == nil {
			writeJSON(w, http.StatusOK, map[string]any{"attempts": []any{}})
			return
		}
		status, attempts := f.attemptsFn(id)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, map[string]any{"attempts": attempts})
		return
	case action == "cancel" && r.Method == http.MethodPost:
		if f.cancelFn == nil {
			http.NotFound(w, r)
			return
		}
		status, job := f.cancelFn(id)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, job)
		return
	case action == "retry" && r.Method == http.MethodPost:
		if f.retryFn == nil {
			http.NotFound(w, r)
			return
		}
		status, job := f.retryFn(id)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, job)
		return
	}

	body := readJSON(r)
	switch action {
	case "heartbeat":
		f.heartbeatCalls.Add(1)
		if f.heartbeatFn == nil {
			writeJSON(w, http.StatusOK, sampleJob(id, nil))
			return
		}
		status, job := f.heartbeatFn(id, body)
		if status >= 400 {
			title := http.StatusText(status)
			detail := "error"
			if status == http.StatusConflict {
				title = "Conflict"
				detail = "lease lost or conflict"
			}
			writeProblem(w, status, title, detail, f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, job)
	case "complete":
		f.completeCalls.Add(1)
		f.inFlight.Add(-1)
		status := http.StatusOK
		if f.completeFn != nil {
			status = f.completeFn(id, body)
		}
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, sampleJob(id, nil))
	case "fail":
		f.failCalls.Add(1)
		f.inFlight.Add(-1)
		f.mu.Lock()
		if errMsg, ok := body["error"].(string); ok {
			f.failErrors = append(f.failErrors, errMsg)
		}
		f.mu.Unlock()
		status := http.StatusOK
		if f.failFn != nil {
			status = f.failFn(id, body)
		}
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, sampleJob(id, nil))
	case "release":
		f.releaseCalls.Add(1)
		status := http.StatusOK
		if f.releaseFn != nil {
			status = f.releaseFn(id, body)
		}
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", f.conflictTypeFor(status))
			return
		}
		writeJSON(w, status, sampleJob(id, nil))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeAPI) conflictTypeFor(status int) string {
	if status == http.StatusConflict {
		if f.conflictType != "" {
			return f.conflictType
		}
		return "https://relay.dev/problems/lease-lost"
	}
	return ""
}

func (f *fakeAPI) handleQueuesRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/v1/queues" {
		http.NotFound(w, r)
		return
	}
	if f.listQueuesFn == nil {
		writeJSON(w, http.StatusOK, map[string]any{"queues": []any{}})
		return
	}
	status, queues := f.listQueuesFn()
	if status >= 400 {
		writeProblem(w, status, http.StatusText(status), "error", "")
		return
	}
	writeJSON(w, status, map[string]any{"queues": queues})
}

func (f *fakeAPI) handleQueues(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/queues/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	queue := parts[0]

	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		if f.queueStatsFn == nil {
			http.NotFound(w, r)
			return
		}
		status, stats := f.queueStatsFn(queue)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", "")
			return
		}
		writeJSON(w, status, stats)
		return
	case len(parts) == 2 && parts[1] == "leases" && r.Method == http.MethodPost:
		f.leaseCalls.Add(1)
		body := readJSON(r)
		if f.leaseFn == nil {
			writeJSON(w, http.StatusOK, map[string]any{"jobs": []any{}})
			return
		}
		status, jobs := f.leaseFn(queue, body)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", "")
			return
		}
		cur := f.inFlight.Add(int32(len(jobs))) //nolint:gosec // lease batch max is 100
		for {
			max := f.maxInFlight.Load()
			if cur <= max || f.maxInFlight.CompareAndSwap(max, cur) {
				break
			}
		}
		writeJSON(w, status, map[string]any{"jobs": jobs})
		return
	case len(parts) == 2 && parts[1] == "pause" && r.Method == http.MethodPost:
		if f.pauseFn == nil {
			http.NotFound(w, r)
			return
		}
		status, q := f.pauseFn(queue)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", "")
			return
		}
		writeJSON(w, status, q)
		return
	case len(parts) == 2 && parts[1] == "resume" && r.Method == http.MethodPost:
		if f.resumeFn == nil {
			http.NotFound(w, r)
			return
		}
		status, q := f.resumeFn(queue)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", "")
			return
		}
		writeJSON(w, status, q)
		return
	case len(parts) == 2 && parts[1] == "jobs" && r.Method == http.MethodGet:
		if f.listJobsFn == nil {
			writeJSON(w, http.StatusOK, map[string]any{"jobs": []any{}, "next_cursor": ""})
			return
		}
		q := r.URL.Query()
		limit := 50
		if v := q.Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				limit = n
			}
		}
		status, jobs, cursor := f.listJobsFn(queue, q.Get("status"), q.Get("cursor"), limit)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", "")
			return
		}
		writeJSON(w, status, map[string]any{"jobs": jobs, "next_cursor": cursor})
		return
	case len(parts) == 3 && parts[1] == "dead" && parts[2] == "retry" && r.Method == http.MethodPost:
		body := readJSON(r)
		if f.retryDeadFn == nil {
			writeJSON(w, http.StatusOK, map[string]any{"retried": 0})
			return
		}
		status, n := f.retryDeadFn(queue, body)
		if status >= 400 {
			writeProblem(w, status, http.StatusText(status), "error", "")
			return
		}
		writeJSON(w, status, map[string]any{"retried": n})
		return
	default:
		http.NotFound(w, r)
	}
}

func readJSON(r *http.Request) map[string]any {
	defer func() { _ = r.Body.Close() }()
	data, _ := io.ReadAll(r.Body)
	if len(data) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	return m
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, title, detail, typ string) {
	if typ == "" {
		typ = "about:blank"
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":   typ,
		"title":  title,
		"status": status,
		"detail": detail,
	})
}

func sampleJob(id string, enqueueBody map[string]any) map[string]any {
	now := time.Now().UTC()
	job := map[string]any{
		"id":               id,
		"queue":            "default",
		"type":             "demo",
		"payload":          map[string]any{},
		"status":           "pending",
		"priority":         0,
		"run_at":           now,
		"attempts":         1,
		"max_attempts":     5,
		"cancel_requested": false,
		"created_at":       now,
		"updated_at":       now,
	}
	if enqueueBody != nil {
		if q, ok := enqueueBody["queue"].(string); ok {
			job["queue"] = q
		}
		if typ, ok := enqueueBody["type"].(string); ok {
			job["type"] = typ
		}
		if p, ok := enqueueBody["payload"]; ok {
			job["payload"] = p
		}
	}
	return job
}

func jobWire(id, queue, typ string, payload any, attempts int) map[string]any {
	now := time.Now().UTC()
	if payload == nil {
		payload = map[string]any{}
	}
	return map[string]any{
		"id":               id,
		"queue":            queue,
		"type":             typ,
		"payload":          payload,
		"status":           "running",
		"priority":         0,
		"run_at":           now,
		"attempts":         attempts,
		"max_attempts":     5,
		"cancel_requested": false,
		"created_at":       now,
		"updated_at":       now,
	}
}
