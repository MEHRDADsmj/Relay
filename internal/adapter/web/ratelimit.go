package web

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// loginLimiter caps login attempts at 5/min per client IP.
type loginLimiter struct {
	mu   sync.Mutex
	byIP map[string]*rate.Limiter
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{byIP: make(map[string]*rate.Limiter)}
}

func (l *loginLimiter) allow(r *http.Request) bool {
	ip := clientIP(r)
	l.mu.Lock()
	lim, ok := l.byIP[ip]
	if !ok {
		lim = rate.NewLimiter(rate.Every(12*time.Second), 5) // 5/min
		l.byIP[ip] = lim
	}
	l.mu.Unlock()
	return lim.Allow()
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
