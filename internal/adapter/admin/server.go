// Package admin serves the private admin HTTP listener (/metrics, pprof, probes).
package admin

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultAddr is the default RELAY_ADMIN_ADDR.
const DefaultAddr = ":7429"

// Options configures the admin HTTP server.
type Options struct {
	Addr     string
	Gatherer prometheus.Gatherer
	// Ready checks storage readiness for /readyz. When nil, /readyz always returns ok.
	Ready func(context.Context) error
}

// Server is the admin HTTP server.
type Server struct {
	addr string
	srv  *http.Server
}

// New builds an admin server with a private mux (never http.DefaultServeMux).
func New(opts Options) *Server {
	addr := opts.Addr
	if addr == "" {
		addr = DefaultAddr
	}
	gatherer := opts.Gatherer
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if opts.Ready != nil {
			if err := opts.Ready(r.Context()); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Explicit pprof handlers on the private mux only.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return &Server{
		addr: addr,
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      60 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

// Addr returns the configured listen address.
func (s *Server) Addr() string { return s.addr }

// Serve listens and serves until Shutdown. lis may be nil (then Listen on Addr).
func (s *Server) Serve(lis net.Listener) error {
	if lis != nil {
		s.srv.Addr = lis.Addr().String()
		return s.srv.Serve(lis)
	}
	return s.srv.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Handler exposes the mux for tests.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
