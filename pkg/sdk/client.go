package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to a Relay HTTP API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithAPIKey sets the Bearer token used for Authorization.
func WithAPIKey(key string) ClientOption {
	return func(c *Client) {
		c.apiKey = key
	}
}

// WithHTTPClient replaces the default HTTP client.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = hc
	}
}

// NewClient builds a Client for baseURL (e.g. "http://localhost:7420").
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Enqueue creates a job (or returns an existing one on idempotent replay).
func (c *Client) Enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error) {
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
	if err := c.doJSON(ctx, http.MethodPost, "/v1/jobs", body, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

// GetJob fetches a job by ID.
func (c *Client) GetJob(ctx context.Context, id string) (JobInfo, error) {
	var out apiJob
	if err := c.doJSON(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &out); err != nil {
		return JobInfo{}, err
	}
	return out.toJobInfo(), nil
}

func (c *Client) lease(ctx context.Context, queue, workerID string, maxJobs int, leaseSeconds int) ([]apiJob, error) {
	body := map[string]any{
		"worker_id": workerID,
		"max_jobs":  maxJobs,
	}
	if leaseSeconds > 0 {
		body["lease_seconds"] = leaseSeconds
	}
	path := "/v1/queues/" + url.PathEscape(queue) + "/leases"
	var out struct {
		Jobs []apiJob `json:"jobs"`
	}
	if err := c.doJSON(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func (c *Client) heartbeat(ctx context.Context, jobID, workerID string, leaseSeconds int) error {
	body := map[string]any{"worker_id": workerID}
	if leaseSeconds > 0 {
		body["lease_seconds"] = leaseSeconds
	}
	return c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/heartbeat", body, nil)
}

func (c *Client) complete(ctx context.Context, jobID, workerID string) error {
	body := map[string]any{"worker_id": workerID}
	return c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/complete", body, nil)
}

func (c *Client) fail(ctx context.Context, jobID, workerID, errMsg string) error {
	body := map[string]any{
		"worker_id": workerID,
		"error":     errMsg,
	}
	return c.doJSON(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/fail", body, nil)
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("relay: marshal request: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	res, err := c.httpClient.Do(req)
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
		Title  string `json:"title"`
		Detail string `json:"detail"`
		Status int    `json:"status"`
	}
	_ = json.Unmarshal(data, &p)
	ae := &APIError{
		StatusCode: status,
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
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       int             `json:"priority"`
	RunAt          time.Time       `json:"run_at"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LeaseOwner     *string         `json:"lease_owner"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at"`
	IdempotencyKey *string         `json:"idempotency_key"`
	LastError      *string         `json:"last_error"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at"`
}

func (j apiJob) toJobInfo() JobInfo {
	info := JobInfo{
		ID:             j.ID,
		Queue:          j.Queue,
		Type:           j.Type,
		Payload:        append([]byte(nil), j.Payload...),
		Status:         j.Status,
		Priority:       j.Priority,
		RunAt:          j.RunAt,
		Attempts:       j.Attempts,
		MaxAttempts:    j.MaxAttempts,
		LeaseExpiresAt: j.LeaseExpiresAt,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		FinishedAt:     j.FinishedAt,
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
	return info
}
