package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type httpTransport struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func newHTTPTransport(baseURL, apiKey string, hc *http.Client) *httpTransport {
	return &httpTransport{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: hc,
	}
}

func (t *httpTransport) close() error { return nil }

func (t *httpTransport) enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error) {
	body := map[string]any{
		"queue": r.Queue,
		"type":  r.Type,
	}
	if r.Payload != nil {
		body["payload"] = r.Payload
	}
	if r.Priority != 0 {
		body["priority"] = r.Priority
	}
	if !r.RunAt.IsZero() {
		body["run_at"] = r.RunAt.UTC()
	}
	if r.MaxAttempts != 0 {
		body["max_attempts"] = r.MaxAttempts
	}
	if r.IdempotencyKey != "" {
		body["idempotency_key"] = r.IdempotencyKey
	}
	var out apiJob
	if err := t.doJSON(ctx, http.MethodPost, "/v1/jobs", body, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

func (t *httpTransport) getJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := t.doJSON(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

func (t *httpTransport) cancelJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := t.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/cancel", nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

func (t *httpTransport) retryJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := t.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/retry", nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

func (t *httpTransport) listAttempts(ctx context.Context, id string) ([]AttemptInfo, error) {
	var out struct {
		Attempts []apiAttempt `json:"attempts"`
	}
	if err := t.doJSON(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id)+"/attempts", nil, &out); err != nil {
		return nil, err
	}
	items := make([]AttemptInfo, 0, len(out.Attempts))
	for _, a := range out.Attempts {
		items = append(items, a.toAttemptInfo())
	}
	return items, nil
}

func (t *httpTransport) listQueues(ctx context.Context) ([]QueueInfo, error) {
	var out struct {
		Queues []apiQueue `json:"queues"`
	}
	if err := t.doJSON(ctx, http.MethodGet, "/v1/queues", nil, &out); err != nil {
		return nil, err
	}
	items := make([]QueueInfo, 0, len(out.Queues))
	for _, q := range out.Queues {
		items = append(items, q.toQueueInfo())
	}
	return items, nil
}

func (t *httpTransport) queueStats(ctx context.Context, queue string) (QueueStatsInfo, error) {
	var out apiQueueStats
	if err := t.doJSON(ctx, http.MethodGet, "/v1/queues/"+url.PathEscape(queue), nil, &out); err != nil {
		return QueueStatsInfo{}, err
	}
	return out.toQueueStatsInfo(), nil
}

func (t *httpTransport) pauseQueue(ctx context.Context, queue string) (QueueInfo, error) {
	var out apiQueue
	if err := t.doJSON(ctx, http.MethodPost, "/v1/queues/"+url.PathEscape(queue)+"/pause", nil, &out); err != nil {
		return QueueInfo{}, err
	}
	return out.toQueueInfo(), nil
}

func (t *httpTransport) resumeQueue(ctx context.Context, queue string) (QueueInfo, error) {
	var out apiQueue
	if err := t.doJSON(ctx, http.MethodPost, "/v1/queues/"+url.PathEscape(queue)+"/resume", nil, &out); err != nil {
		return QueueInfo{}, err
	}
	return out.toQueueInfo(), nil
}

func (t *httpTransport) retryDead(ctx context.Context, queue string, limit int) (int, error) {
	body := map[string]any{}
	if limit > 0 {
		body["limit"] = limit
	}
	var out struct {
		Retried int `json:"retried"`
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/dead/retry"
	if err := t.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return 0, err
	}
	return out.Retried, nil
}

func (t *httpTransport) listJobsPage(ctx context.Context, queue, status, cursor string, limit int) ([]JobInfo, string, error) {
	q := url.Values{}
	q.Set("status", status)
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/jobs?" + q.Encode()
	var out struct {
		Jobs       []apiJob `json:"jobs"`
		NextCursor string   `json:"next_cursor"`
	}
	if err := t.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, "", err
	}
	items := make([]JobInfo, 0, len(out.Jobs))
	for _, j := range out.Jobs {
		items = append(items, j.toJobInfo())
	}
	return items, out.NextCursor, nil
}

func (t *httpTransport) lease(ctx context.Context, queue, workerID string, maxJobs, leaseSeconds, waitSeconds int) ([]apiJob, error) {
	body := map[string]any{
		"worker_id": workerID,
		"max_jobs":  maxJobs,
	}
	if leaseSeconds > 0 {
		body["lease_seconds"] = leaseSeconds
	}
	if waitSeconds > 0 {
		body["wait_seconds"] = waitSeconds
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/leases"
	var out struct {
		Jobs []apiJob `json:"jobs"`
	}
	if err := t.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func (t *httpTransport) heartbeat(ctx context.Context, jobID, workerID string, leaseSeconds int) (apiJob, error) {
	body := map[string]any{"worker_id": workerID}
	if leaseSeconds > 0 {
		body["lease_seconds"] = leaseSeconds
	}
	var out apiJob
	if err := t.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/heartbeat", body, &out); err != nil {
		return apiJob{}, err
	}
	return out, nil
}

func (t *httpTransport) complete(ctx context.Context, jobID, workerID string) error {
	body := map[string]any{"worker_id": workerID}
	return t.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/complete", body, nil)
}

func (t *httpTransport) fail(ctx context.Context, jobID, workerID, errMsg string, permanent bool) error {
	body := map[string]any{
		"worker_id": workerID,
		"error":     errMsg,
	}
	if permanent {
		body["permanent"] = true
	}
	return t.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/fail", body, nil)
}

func (t *httpTransport) release(ctx context.Context, jobID, workerID string) error {
	body := map[string]any{"worker_id": workerID}
	return t.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/release", body, nil)
}

func (t *httpTransport) createSchedule(ctx context.Context, r CreateScheduleRequest) (ScheduleInfo, error) {
	body := map[string]any{
		"name":  r.Name,
		"cron":  r.Cron,
		"queue": r.Queue,
		"type":  r.Type,
	}
	if r.Timezone != "" {
		body["timezone"] = r.Timezone
	}
	if r.Payload != nil {
		body["payload"] = r.Payload
	}
	if r.Priority != 0 {
		body["priority"] = r.Priority
	}
	if r.MaxAttempts != 0 {
		body["max_attempts"] = r.MaxAttempts
	}
	if r.Enabled != nil {
		body["enabled"] = *r.Enabled
	}
	var out apiSchedule
	if err := t.doJSON(ctx, http.MethodPost, "/v1/schedules", body, &out); err != nil {
		return ScheduleInfo{}, err
	}
	return out.toScheduleInfo(), nil
}

func (t *httpTransport) getSchedule(ctx context.Context, idOrName string) (ScheduleInfo, error) {
	var out apiSchedule
	if err := t.doJSON(ctx, http.MethodGet, "/v1/schedules/"+url.PathEscape(idOrName), nil, &out); err != nil {
		return ScheduleInfo{}, err
	}
	return out.toScheduleInfo(), nil
}

func (t *httpTransport) listSchedules(ctx context.Context, opts ListSchedulesOptions) ([]ScheduleInfo, string, error) {
	q := url.Values{}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Cursor != "" {
		q.Set("cursor", opts.Cursor)
	}
	path := "/v1/schedules"
	if enc := q.Encode(); enc != "" {
		path += "?" + enc
	}
	var out struct {
		Schedules  []apiSchedule `json:"schedules"`
		NextCursor string        `json:"next_cursor"`
	}
	if err := t.doJSON(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, "", err
	}
	items := make([]ScheduleInfo, 0, len(out.Schedules))
	for _, s := range out.Schedules {
		items = append(items, s.toScheduleInfo())
	}
	return items, out.NextCursor, nil
}

func (t *httpTransport) updateSchedule(ctx context.Context, id string, r UpdateScheduleRequest) (ScheduleInfo, error) {
	body := map[string]any{}
	if r.Cron != nil {
		body["cron"] = *r.Cron
	}
	if r.Timezone != nil {
		body["timezone"] = *r.Timezone
	}
	if r.Queue != nil {
		body["queue"] = *r.Queue
	}
	if r.Type != nil {
		body["type"] = *r.Type
	}
	if r.SetPayload {
		body["payload"] = json.RawMessage(r.Payload)
		if r.Payload == nil {
			body["payload"] = json.RawMessage("{}")
		}
	}
	if r.Priority != nil {
		body["priority"] = *r.Priority
	}
	if r.MaxAttempts != nil {
		body["max_attempts"] = *r.MaxAttempts
	}
	if r.Enabled != nil {
		body["enabled"] = *r.Enabled
	}
	headers := map[string]string{}
	if !r.ExpectedUpdatedAt.IsZero() {
		headers["If-Match"] = r.ExpectedUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	var out apiSchedule
	if err := t.doJSONHeaders(ctx, http.MethodPatch, "/v1/schedules/"+url.PathEscape(id), body, headers, &out); err != nil {
		return ScheduleInfo{}, err
	}
	return out.toScheduleInfo(), nil
}

func (t *httpTransport) deleteSchedule(ctx context.Context, id string) error {
	return t.doJSON(ctx, http.MethodDelete, "/v1/schedules/"+url.PathEscape(id), nil, nil)
}

func (t *httpTransport) enableSchedule(ctx context.Context, id string) (ScheduleInfo, error) {
	var out apiSchedule
	if err := t.doJSON(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(id)+"/enable", nil, &out); err != nil {
		return ScheduleInfo{}, err
	}
	return out.toScheduleInfo(), nil
}

func (t *httpTransport) disableSchedule(ctx context.Context, id string) (ScheduleInfo, error) {
	var out apiSchedule
	if err := t.doJSON(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(id)+"/disable", nil, &out); err != nil {
		return ScheduleInfo{}, err
	}
	return out.toScheduleInfo(), nil
}

func (t *httpTransport) triggerSchedule(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := t.doJSON(ctx, http.MethodPost, "/v1/schedules/"+url.PathEscape(id)+"/trigger", nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

func (t *httpTransport) createAPIKey(ctx context.Context, r CreateAPIKeyRequest) (CreatedAPIKey, error) {
	body := map[string]any{
		"name":   r.Name,
		"scopes": r.Scopes,
	}
	if r.ExpiresAt != nil {
		body["expires_at"] = r.ExpiresAt.UTC()
	}
	var out struct {
		Key    apiKeyWire `json:"key"`
		Secret string     `json:"secret"`
	}
	if err := t.doJSON(ctx, http.MethodPost, "/v1/api-keys", body, &out); err != nil {
		return CreatedAPIKey{}, err
	}
	return CreatedAPIKey{Key: out.Key.toAPIKeyInfo(), Secret: out.Secret}, nil
}

func (t *httpTransport) listAPIKeys(ctx context.Context) ([]APIKeyInfo, error) {
	var out struct {
		Keys []apiKeyWire `json:"keys"`
	}
	if err := t.doJSON(ctx, http.MethodGet, "/v1/api-keys", nil, &out); err != nil {
		return nil, err
	}
	items := make([]APIKeyInfo, 0, len(out.Keys))
	for _, k := range out.Keys {
		items = append(items, k.toAPIKeyInfo())
	}
	return items, nil
}

func (t *httpTransport) revokeAPIKey(ctx context.Context, id string) (APIKeyInfo, error) {
	var out apiKeyWire
	if err := t.doJSON(ctx, http.MethodDelete, "/v1/api-keys/"+url.PathEscape(id), nil, &out); err != nil {
		return APIKeyInfo{}, err
	}
	return out.toAPIKeyInfo(), nil
}

func (t *httpTransport) doJSON(ctx context.Context, method, path string, body any, out any) error {
	return t.doJSONHeaders(ctx, method, path, body, nil, out)
}

func (t *httpTransport) doJSONHeaders(ctx context.Context, method, path string, body any, extra map[string]string, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("relay: marshal request: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if t.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+t.apiKey)
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	res, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		return parseAPIError(res.StatusCode, data)
	}
	if out == nil || res.StatusCode == http.StatusNoContent || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("relay: decode response: %w", err)
	}
	return nil
}

func parseAPIError(status int, data []byte) error {
	var p struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	}
	_ = json.Unmarshal(data, &p)
	ae := &APIError{
		StatusCode: status,
		Type:       p.Type,
		Title:      p.Title,
		Detail:     p.Detail,
	}
	if ae.Title == "" {
		ae.Title = http.StatusText(status)
	}
	return ae
}

// apiJob is the wire shape of a Job from the HTTP API.
type apiJob struct {
	ID              string            `json:"id"`
	Queue           string            `json:"queue"`
	Type            string            `json:"type"`
	Payload         json.RawMessage   `json:"payload"`
	Status          string            `json:"status"`
	Priority        int               `json:"priority"`
	RunAt           time.Time         `json:"run_at"`
	Attempts        int               `json:"attempts"`
	MaxAttempts     int               `json:"max_attempts"`
	LeaseOwner      *string           `json:"lease_owner"`
	LeaseExpiresAt  *time.Time        `json:"lease_expires_at"`
	IdempotencyKey  *string           `json:"idempotency_key"`
	LastError       *string           `json:"last_error"`
	CancelRequested bool              `json:"cancel_requested"`
	CancelledAt     *time.Time        `json:"cancelled_at"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	FinishedAt      *time.Time        `json:"finished_at"`
	TraceContext    map[string]string `json:"trace_context"`
}

func (j apiJob) toJobInfo() JobInfo {
	info := JobInfo{
		ID:              j.ID,
		Queue:           j.Queue,
		Type:            j.Type,
		Payload:         append([]byte(nil), j.Payload...),
		Status:          j.Status,
		Priority:        j.Priority,
		RunAt:           j.RunAt,
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		LeaseExpiresAt:  j.LeaseExpiresAt,
		CancelRequested: j.CancelRequested,
		CancelledAt:     j.CancelledAt,
		CreatedAt:       j.CreatedAt,
		UpdatedAt:       j.UpdatedAt,
		FinishedAt:      j.FinishedAt,
	}
	if j.LeaseOwner != nil {
		info.LeaseOwner = *j.LeaseOwner
	}
	if j.IdempotencyKey != nil {
		info.IdempotencyKey = *j.IdempotencyKey
	}
	if j.LastError != nil {
		info.LastError = *j.LastError
	}
	if len(j.TraceContext) > 0 {
		info.TraceContext = make(map[string]string, len(j.TraceContext))
		for k, v := range j.TraceContext {
			info.TraceContext[k] = v
		}
	}
	return info
}

type apiAttempt struct {
	JobID      string     `json:"job_id"`
	Number     int        `json:"number"`
	WorkerID   string     `json:"worker_id"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Outcome    string     `json:"outcome"`
	Error      string     `json:"error"`
}

func (a apiAttempt) toAttemptInfo() AttemptInfo {
	return AttemptInfo(a)
}

type apiQueue struct {
	Name      string    `json:"name"`
	Paused    bool      `json:"paused"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (q apiQueue) toQueueInfo() QueueInfo {
	return QueueInfo(q)
}

type apiQueueStats struct {
	Queue                   string `json:"queue"`
	Paused                  bool   `json:"paused"`
	Pending                 int64  `json:"pending"`
	Scheduled               int64  `json:"scheduled"`
	Running                 int64  `json:"running"`
	Succeeded               int64  `json:"succeeded"`
	Dead                    int64  `json:"dead"`
	Cancelled               int64  `json:"cancelled"`
	OldestPendingAgeSeconds int64  `json:"oldest_pending_age_seconds"`
}

func (s apiQueueStats) toQueueStatsInfo() QueueStatsInfo {
	return QueueStatsInfo{
		Queue:            s.Queue,
		Paused:           s.Paused,
		Pending:          s.Pending,
		Scheduled:        s.Scheduled,
		Running:          s.Running,
		Succeeded:        s.Succeeded,
		Dead:             s.Dead,
		Cancelled:        s.Cancelled,
		OldestPendingAge: time.Duration(s.OldestPendingAgeSeconds) * time.Second,
	}
}

type apiSchedule struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Timezone    string          `json:"timezone"`
	Queue       string          `json:"queue"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
	Enabled     bool            `json:"enabled"`
	NextRunAt   time.Time       `json:"next_run_at"`
	LastRunAt   *time.Time      `json:"last_run_at"`
	LastJobID   *string         `json:"last_job_id"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func (s apiSchedule) toScheduleInfo() ScheduleInfo {
	info := ScheduleInfo{
		ID:          s.ID,
		Name:        s.Name,
		Cron:        s.Cron,
		Timezone:    s.Timezone,
		Queue:       s.Queue,
		Type:        s.Type,
		Payload:     append([]byte(nil), s.Payload...),
		Priority:    s.Priority,
		MaxAttempts: s.MaxAttempts,
		Enabled:     s.Enabled,
		NextRunAt:   s.NextRunAt,
		LastRunAt:   s.LastRunAt,
		CreatedAt:   s.CreatedAt,
		UpdatedAt:   s.UpdatedAt,
	}
	if s.LastJobID != nil {
		info.LastJobID = *s.LastJobID
	}
	return info
}

type apiKeyWire struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	Scopes     []string   `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
}

func (k apiKeyWire) toAPIKeyInfo() APIKeyInfo {
	return APIKeyInfo{
		ID:         k.ID,
		Name:       k.Name,
		Prefix:     k.Prefix,
		Scopes:     append([]string(nil), k.Scopes...),
		CreatedAt:  k.CreatedAt,
		LastUsedAt: k.LastUsedAt,
		ExpiresAt:  k.ExpiresAt,
		RevokedAt:  k.RevokedAt,
	}
}
