package grpc

import (
	"context"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	relayv1 "github.com/MEHRDADsmj/relay/gen/relay/v1"
	"github.com/MEHRDADsmj/relay/internal/app"
)

// DefaultAddr is the default listen address for RELAY_GRPC_ADDR.
const DefaultAddr = ":7421"

// Options configures the gRPC server.
type Options struct {
	// Auth verifies Bearer tokens and attaches a Principal. When nil, all RPCs
	// get BootstrapAdmin (open mode for tests).
	Auth *app.AuthService
	// Logger is used for access logs and panic recovery. Defaults to slog.Default.
	Logger *slog.Logger
	// Reflection enables gRPC server reflection (RELAY_GRPC_REFLECTION).
	Reflection bool
	// Ready reports storage readiness for the health service. When nil, health
	// always reports SERVING.
	Ready func(context.Context) error
}

// Services holds app-layer dependencies for the gRPC adapter.
type Services struct {
	Jobs      *app.JobService
	Queues    *app.QueueService
	Schedules *app.ScheduleService
	APIKeys   *app.APIKeyService
}

// NewServer builds a *grpc.Server with Relay services, interceptors, health,
// and optional reflection. Ready for T9 wiring: dial with Serve(lis).
func NewServer(svc Services, opts Options) *grpc.Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s := grpc.NewServer(
		grpc.UnaryInterceptor(unaryInterceptorChain(opts.Auth, opts.Logger)),
		grpc.StreamInterceptor(streamInterceptorChain(opts.Auth, opts.Logger)),
	)

	relayv1.RegisterJobServiceServer(s, &jobServer{jobs: svc.Jobs})
	relayv1.RegisterQueueServiceServer(s, &queueServer{queues: svc.Queues, jobs: svc.Jobs})
	relayv1.RegisterScheduleServiceServer(s, &scheduleServer{schedules: svc.Schedules})
	relayv1.RegisterAPIKeyServiceServer(s, &apiKeyServer{apiKeys: svc.APIKeys})

	grpc_health_v1.RegisterHealthServer(s, &storageHealth{ready: opts.Ready})

	if opts.Reflection {
		reflection.Register(s)
	}
	return s
}

// GracefulStop stops the server: GracefulStop with timeout, then Stop.
func GracefulStop(s *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		s.Stop()
	}
}

// Listen opens a TCP listener on addr (default DefaultAddr).
func Listen(addr string) (net.Listener, error) {
	if addr == "" {
		addr = DefaultAddr
	}
	return net.Listen("tcp", addr)
}

type storageHealth struct {
	grpc_health_v1.UnimplementedHealthServer
	ready func(context.Context) error
}

func (h *storageHealth) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if h.ready != nil {
		if err := h.ready(ctx); err != nil {
			return &grpc_health_v1.HealthCheckResponse{
				Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
			}, nil
		}
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

func (h *storageHealth) Watch(_ *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	status := grpc_health_v1.HealthCheckResponse_SERVING
	if h.ready != nil {
		if err := h.ready(stream.Context()); err != nil {
			status = grpc_health_v1.HealthCheckResponse_NOT_SERVING
		}
	}
	if err := stream.Send(&grpc_health_v1.HealthCheckResponse{Status: status}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}
