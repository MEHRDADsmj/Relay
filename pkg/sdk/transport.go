package sdk

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/grpc"
)

// TransportKind selects the wire protocol used by Client.
type TransportKind int

const (
	// TransportHTTP uses the REST JSON API.
	TransportHTTP TransportKind = iota + 1
	// TransportGRPC uses the gRPC API (gen/relay/v1).
	TransportGRPC
)

// transport is the internal wire abstraction behind Client.
type transport interface {
	enqueue(ctx context.Context, r EnqueueRequest) (JobInfo, error)
	getJob(ctx context.Context, id string) (JobInfo, error)
	cancelJob(ctx context.Context, id string) (JobInfo, error)
	retryJob(ctx context.Context, id string) (JobInfo, error)
	listAttempts(ctx context.Context, id string) ([]AttemptInfo, error)

	listQueues(ctx context.Context) ([]QueueInfo, error)
	queueStats(ctx context.Context, queue string) (QueueStatsInfo, error)
	pauseQueue(ctx context.Context, queue string) (QueueInfo, error)
	resumeQueue(ctx context.Context, queue string) (QueueInfo, error)
	retryDead(ctx context.Context, queue string, limit int) (int, error)
	listJobsPage(ctx context.Context, queue, status, cursor string, limit int) ([]JobInfo, string, error)

	lease(ctx context.Context, queue, workerID string, maxJobs, leaseSeconds, waitSeconds int) ([]apiJob, error)
	heartbeat(ctx context.Context, jobID, workerID string, leaseSeconds int) (apiJob, error)
	complete(ctx context.Context, jobID, workerID string) error
	fail(ctx context.Context, jobID, workerID, errMsg string, permanent bool) error
	release(ctx context.Context, jobID, workerID string) error

	createSchedule(ctx context.Context, r CreateScheduleRequest) (ScheduleInfo, error)
	getSchedule(ctx context.Context, idOrName string) (ScheduleInfo, error)
	listSchedules(ctx context.Context, opts ListSchedulesOptions) ([]ScheduleInfo, string, error)
	updateSchedule(ctx context.Context, id string, r UpdateScheduleRequest) (ScheduleInfo, error)
	deleteSchedule(ctx context.Context, id string) error
	enableSchedule(ctx context.Context, id string) (ScheduleInfo, error)
	disableSchedule(ctx context.Context, id string) (ScheduleInfo, error)
	triggerSchedule(ctx context.Context, id string) (JobInfo, error)

	createAPIKey(ctx context.Context, r CreateAPIKeyRequest) (CreatedAPIKey, error)
	listAPIKeys(ctx context.Context) ([]APIKeyInfo, error)
	revokeAPIKey(ctx context.Context, id string) (APIKeyInfo, error)

	close() error
}

type clientOptions struct {
	apiKey       string
	httpClient   *http.Client
	kind         TransportKind // 0 = detect from scheme
	grpcDialOpts []grpc.DialOption
}

// WithTransport forces HTTP or gRPC regardless of the target URL scheme.
func WithTransport(kind TransportKind) ClientOption {
	return func(c *Client) {
		c.opts.kind = kind
	}
}

// WithGRPCDialOptions appends dial options used when building a gRPC transport
// (e.g. bufconn dialers in tests, custom TLS).
func WithGRPCDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(c *Client) {
		c.opts.grpcDialOpts = append(c.opts.grpcDialOpts, opts...)
	}
}

func detectKind(target string, forced TransportKind) (TransportKind, error) {
	if forced != 0 {
		return forced, nil
	}
	u, err := url.Parse(target)
	if err != nil {
		return 0, fmt.Errorf("relay: parse target: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "":
		return TransportHTTP, nil
	case "grpc", "grpcs":
		return TransportGRPC, nil
	default:
		return 0, fmt.Errorf("relay: unsupported target scheme %q (use http(s):// or grpc(s)://)", u.Scheme)
	}
}

func buildTransport(target string, opts clientOptions) (transport, error) {
	kind, err := detectKind(target, opts.kind)
	if err != nil {
		return nil, err
	}
	switch kind {
	case TransportHTTP:
		base := target
		if opts.kind == TransportHTTP {
			// Allow grpc://host overridden to HTTP by rewriting to http://host.
			if u, err := url.Parse(target); err == nil {
				switch strings.ToLower(u.Scheme) {
				case "grpc":
					u.Scheme = "http"
					base = u.String()
				case "grpcs":
					u.Scheme = "https"
					base = u.String()
				}
			}
		}
		hc := opts.httpClient
		if hc == nil {
			hc = http.DefaultClient
		}
		return newHTTPTransport(base, opts.apiKey, hc), nil
	case TransportGRPC:
		return newGRPCTransport(target, opts.apiKey, opts.grpcDialOpts...)
	default:
		return nil, fmt.Errorf("relay: unknown transport kind %d", kind)
	}
}
