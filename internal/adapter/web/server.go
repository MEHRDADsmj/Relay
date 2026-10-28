package web

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/go-chi/chi/v5"

	"github.com/MEHRDADsmj/relay/internal/adapter/web/templates"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type ctxKey int

const sessionCtxKey ctxKey = 1

// Options configures the dashboard adapter.
type Options struct {
	Auth          *app.AuthService
	Logger        *slog.Logger
	SessionKey    []byte // 32 bytes; generated if empty
	SecureCookies bool
	Clock         func() time.Time // optional; defaults to time.Now
}

// Server is the /ui dashboard HTTP adapter.
type Server struct {
	auth     *app.AuthService
	log      *slog.Logger
	sessions *sessionStore
	loginRL  *loginLimiter
	handler  http.Handler
}

// New builds the dashboard handler. When SessionKey is empty a random key is
// generated and returned so the caller can log a development warning.
func New(opts Options) (*Server, []byte, error) {
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	key := append([]byte(nil), opts.SessionKey...)
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, nil, err
		}
	}
	if len(key) != 32 {
		return nil, nil, errors.New("web: session key must be 32 bytes")
	}
	store, err := newSessionStore(key, opts.SecureCookies, opts.Clock)
	if err != nil {
		return nil, nil, err
	}
	s := &Server{
		auth:     opts.Auth,
		log:      log,
		sessions: store,
		loginRL:  newLoginLimiter(),
	}
	s.handler = s.routes()
	return s, key, nil
}

// Handler returns the mounted /ui HTTP handler.
func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(securityHeaders)

	r.Handle("/ui/static/*", staticHandler())

	r.Get("/ui/login", s.handleLoginGet)
	r.Post("/ui/login", s.handleLoginPost)
	r.With(s.requireSession, s.requireCSRF).Post("/ui/logout", s.handleLogout)

	r.With(s.requireSession).Get("/ui/", s.handleHome)
	r.With(s.requireSession).Get("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusSeeOther)
	})

	r.NotFound(s.handleNotFound)
	return r
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r.Context())
	p, _ := app.PrincipalFrom(r.Context())
	name := p.Name
	if name == "" {
		name = sess.KeyID
	}
	s.render(w, r, http.StatusOK, templates.HomePage(sess.CSRF, name))
}

func (s *Server) handleLoginGet(w http.ResponseWriter, r *http.Request) {
	if sess, err := s.sessions.fromRequest(r); err == nil && sess.KeyID != "" {
		if p, err := s.auth.PrincipalByKeyID(r.Context(), sess.KeyID); err == nil && p.Can(domain.ScopeDashboardRead) {
			http.Redirect(w, r, "/ui/", http.StatusSeeOther)
			return
		}
	}
	payload, err := s.sessions.issue("")
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not start a login session.")
		return
	}
	if err := s.sessions.setCookie(w, payload); err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not start a login session.")
		return
	}
	s.render(w, r, http.StatusOK, templates.LoginPage(payload.CSRF, ""))
}

func (s *Server) handleLoginPost(w http.ResponseWriter, r *http.Request) {
	if !s.loginRL.allow(r) {
		s.renderLoginError(w, r, http.StatusTooManyRequests, "Too many login attempts. Try again in a minute.")
		return
	}
	if err := r.ParseForm(); err != nil {
		s.renderLoginError(w, r, http.StatusBadRequest, "Invalid form.")
		return
	}
	sess, err := s.sessions.fromRequest(r)
	if err != nil || !csrfOK(sess.CSRF, r) {
		s.renderLoginError(w, r, http.StatusForbidden, "Invalid or missing CSRF token. Reload the page and try again.")
		return
	}
	secret := strings.TrimSpace(r.FormValue("api_key"))
	if secret == "" {
		s.renderLoginError(w, r, http.StatusUnauthorized, "API key is required.")
		return
	}
	if s.auth == nil {
		s.renderLoginError(w, r, http.StatusInternalServerError, "Auth is not configured.")
		return
	}
	p, err := s.auth.Authenticate(r.Context(), secret)
	if err != nil {
		s.renderLoginError(w, r, http.StatusUnauthorized, "Invalid API key.")
		return
	}
	if !p.Can(domain.ScopeDashboardRead) {
		s.renderLoginError(w, r, http.StatusForbidden, "API key lacks dashboard access.")
		return
	}
	payload, err := s.sessions.issue(p.KeyID)
	if err != nil {
		s.renderLoginError(w, r, http.StatusInternalServerError, "Could not create session.")
		return
	}
	if err := s.sessions.setCookie(w, payload); err != nil {
		s.renderLoginError(w, r, http.StatusInternalServerError, "Could not create session.")
		return
	}
	http.Redirect(w, r, "/ui/", http.StatusSeeOther)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.sessions.clearCookie(w)
	http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	s.renderError(w, r, http.StatusNotFound, "Page not found", "That dashboard page does not exist.")
}

func (s *Server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := s.sessions.fromRequest(r)
		if err != nil || sess.KeyID == "" {
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		if s.auth == nil {
			s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Auth is not configured.")
			return
		}
		p, err := s.auth.PrincipalByKeyID(r.Context(), sess.KeyID)
		if err != nil {
			s.sessions.clearCookie(w)
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		if !p.Can(domain.ScopeDashboardRead) {
			s.sessions.clearCookie(w)
			http.Redirect(w, r, "/ui/login", http.StatusSeeOther)
			return
		}
		ctx := app.WithPrincipal(r.Context(), p)
		ctx = context.WithValue(ctx, sessionCtxKey, sess)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) requireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess := sessionFrom(r.Context())
		if !csrfOK(sess.CSRF, r) {
			s.renderError(w, r, http.StatusForbidden, "Forbidden", "Invalid or missing CSRF token.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func csrfOK(want string, r *http.Request) bool {
	got := r.Header.Get("X-CSRF-Token")
	if got == "" {
		_ = r.ParseForm()
		got = r.FormValue("csrf")
	}
	if want == "" || got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(want), []byte(got)) == 1
}

func sessionFrom(ctx context.Context) SessionPayload {
	s, _ := ctx.Value(sessionCtxKey).(SessionPayload)
	return s
}

func (s *Server) renderLoginError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	payload, err := s.sessions.issue("")
	csrf := ""
	if err == nil {
		csrf = payload.CSRF
		_ = s.sessions.setCookie(w, payload)
	}
	s.render(w, r, status, templates.LoginPage(csrf, msg))
}

func (s *Server) renderError(w http.ResponseWriter, r *http.Request, status int, title, message string) {
	s.render(w, r, status, templates.ErrorPage(status, title, message))
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, status int, component templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := component.Render(r.Context(), w); err != nil {
		s.log.ErrorContext(r.Context(), "render template", "error", err, "path", r.URL.Path)
	}
}

// EncodeSessionKey returns standard base64 for logging/config samples.
func EncodeSessionKey(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
}
