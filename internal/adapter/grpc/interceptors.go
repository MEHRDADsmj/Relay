package grpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

const (
	mdAuthorization = "authorization"
	mdRequestID     = "x-request-id"
	bearerPrefix    = "bearer "
)

type ctxKey int

const requestIDKey ctxKey = 1

// RequestIDFromContext returns the request ID set by the interceptor, or "".
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

func unaryInterceptorChain(auth *app.AuthService, log *slog.Logger) grpc.UnaryServerInterceptor {
	return chainUnary(
		recoverUnary(log),
		requestIDUnary(),
		accessLogUnary(log),
		authUnary(auth),
	)
}

func streamInterceptorChain(auth *app.AuthService, log *slog.Logger) grpc.StreamServerInterceptor {
	return chainStream(
		recoverStream(log),
		requestIDStream(),
		accessLogStream(log),
		authStream(auth),
	)
}

func chainUnary(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		build := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			next := build
			interceptor := interceptors[i]
			build = func(ctx context.Context, req any) (any, error) {
				return interceptor(ctx, req, info, next)
			}
		}
		return build(ctx, req)
	}
}

func chainStream(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		build := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			next := build
			interceptor := interceptors[i]
			build = func(srv any, ss grpc.ServerStream) error {
				return interceptor(srv, ss, info, next)
			}
		}
		return build(srv, ss)
	}
}

func recoverUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				log.ErrorContext(ctx, "panic recovered",
					"panic", rec,
					"method", info.FullMethod,
					"request_id", RequestIDFromContext(ctx),
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "an unexpected error occurred")
				resp = nil
			}
		}()
		return handler(ctx, req)
	}
}

func recoverStream(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				log.ErrorContext(ss.Context(), "panic recovered",
					"panic", rec,
					"method", info.FullMethod,
					"request_id", RequestIDFromContext(ss.Context()),
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "an unexpected error occurred")
			}
		}()
		return handler(srv, ss)
	}
}

func requestIDUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id := requestIDFromMD(ctx)
		ctx = context.WithValue(ctx, requestIDKey, id)
		_ = grpc.SetHeader(ctx, metadata.Pairs(mdRequestID, id))
		return handler(ctx, req)
	}
}

func requestIDStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		id := requestIDFromMD(ss.Context())
		ctx := context.WithValue(ss.Context(), requestIDKey, id)
		wrapped := &ctxServerStream{ServerStream: ss, ctx: ctx}
		_ = ss.SetHeader(metadata.Pairs(mdRequestID, id))
		return handler(srv, wrapped)
	}
}

func requestIDFromMD(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(mdRequestID); len(vals) > 0 && strings.TrimSpace(vals[0]) != "" {
			return strings.TrimSpace(vals[0])
		}
	}
	return newRequestID()
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}

func accessLogUnary(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		attrs := []any{
			"method", info.FullMethod,
			"code", code.String(),
			"duration", time.Since(start),
			"request_id", RequestIDFromContext(ctx),
		}
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			attrs = append(attrs, "peer", p.Addr.String())
		}
		log.InfoContext(ctx, "access", attrs...)
		return resp, err
	}
}

func accessLogStream(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		log.InfoContext(ss.Context(), "access",
			"method", info.FullMethod,
			"code", code.String(),
			"duration", time.Since(start),
			"request_id", RequestIDFromContext(ss.Context()),
		)
		return err
	}
}

func authUnary(auth *app.AuthService) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isPublicMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, err := authenticate(ctx, auth)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func authStream(auth *app.AuthService) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isPublicMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, err := authenticate(ss.Context(), auth)
		if err != nil {
			return err
		}
		return handler(srv, &ctxServerStream{ServerStream: ss, ctx: ctx})
	}
}

func isPublicMethod(fullMethod string) bool {
	// Health + reflection must remain reachable without credentials.
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection.")
}

func authenticate(ctx context.Context, auth *app.AuthService) (context.Context, error) {
	if auth == nil {
		return app.WithPrincipal(ctx, app.BootstrapAdmin), nil
	}
	secret, hasBearer := bearerSecret(ctx)
	if !hasBearer {
		open, err := auth.IsOpen(ctx)
		if err != nil {
			return nil, status.Error(codes.Internal, "an unexpected error occurred")
		}
		if open {
			return app.WithPrincipal(ctx, app.BootstrapAdmin), nil
		}
		return nil, status.Error(codes.Unauthenticated, "missing or invalid API key")
	}
	p, err := auth.Authenticate(ctx, secret)
	if err != nil {
		if errors.Is(err, domain.ErrUnauthenticated) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid API key")
		}
		return nil, status.Error(codes.Internal, "an unexpected error occurred")
	}
	return app.WithPrincipal(ctx, p), nil
}

func bearerSecret(ctx context.Context) (secret string, ok bool) {
	md, mdOK := metadata.FromIncomingContext(ctx)
	if !mdOK {
		return "", false
	}
	vals := md.Get(mdAuthorization)
	if len(vals) == 0 {
		return "", false
	}
	raw := strings.TrimSpace(vals[0])
	if len(raw) < len(bearerPrefix) || !strings.EqualFold(raw[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}
	secret = strings.TrimSpace(raw[len(bearerPrefix):])
	if secret == "" {
		return "", false
	}
	return secret, true
}

type ctxServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxServerStream) Context() context.Context { return s.ctx }
