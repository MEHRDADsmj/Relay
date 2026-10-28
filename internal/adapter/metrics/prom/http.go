package prom

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// HTTPMetrics records relay_http_requests_total and relay_http_request_duration_seconds.
type HTTPMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewHTTPMetrics registers HTTP metrics on reg.
func NewHTTPMetrics(reg prometheus.Registerer) (*HTTPMetrics, error) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &HTTPMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "relay",
			Name:      "http_requests_total",
			Help:      "HTTP requests handled.",
		}, []string{"route", "method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "relay",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request latency.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"route", "method"}),
	}
	if err := reg.Register(m.requests); err != nil {
		return nil, err
	}
	if err := reg.Register(m.duration); err != nil {
		return nil, err
	}
	return m, nil
}

type statusCapture struct {
	http.ResponseWriter
	status int
}

func (s *statusCapture) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Middleware returns chi-compatible middleware that records HTTP metrics after the handler.
func (m *HTTPMetrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusCapture{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		code := strconv.Itoa(rec.status)
		m.requests.WithLabelValues(route, r.Method, code).Inc()
		m.duration.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}
