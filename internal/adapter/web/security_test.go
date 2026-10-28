package web_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/adapter/web"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestUI_UnauthenticatedRedirect(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")

	res := env.do(t, http.MethodGet, "/ui/", nil, nil)
	assert.Equal(t, http.StatusSeeOther, res.Code)
	assert.Equal(t, "/ui/login", res.Header().Get("Location"))
}

func TestUI_SecurityHeaders(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")

	res := env.do(t, http.MethodGet, "/ui/login", nil, nil)
	require.Equal(t, http.StatusOK, res.Code)
	assert.Contains(t, res.Header().Get("Content-Security-Policy"), "default-src 'self'")
	assert.Contains(t, res.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
	assert.Equal(t, "nosniff", res.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "same-origin", res.Header().Get("Referrer-Policy"))

	found := false
	for _, c := range res.Result().Cookies() {
		if c.Name == "relay_ui_session" {
			assert.True(t, c.HttpOnly)
			assert.Equal(t, "/ui", c.Path)
			assert.Equal(t, http.SameSiteLaxMode, c.SameSite)
			found = true
		}
	}
	require.True(t, found)
}

func TestUI_LoginLogoutRoundTrip(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")

	login := env.do(t, http.MethodGet, "/ui/login", nil, nil)
	require.Equal(t, http.StatusOK, login.Code)
	csrf := extractCSRF(t, login.Body.String())
	cookies := login.Result().Cookies()

	posted := env.do(t, http.MethodPost, "/ui/login", url.Values{
		"csrf":    {csrf},
		"api_key": {"admin-secret"},
	}, cookies)
	require.Equal(t, http.StatusSeeOther, posted.Code)
	assert.Equal(t, "/ui/", posted.Header().Get("Location"))
	cookies = mergeCookies(cookies, posted.Result().Cookies())

	home := env.do(t, http.MethodGet, "/ui/", nil, cookies)
	require.Equal(t, http.StatusOK, home.Code)
	assert.Contains(t, home.Body.String(), "Overview")
	assert.Contains(t, home.Body.String(), "bootstrap")

	logoutCSRF := extractCSRF(t, home.Body.String())
	logout := env.do(t, http.MethodPost, "/ui/logout", url.Values{"csrf": {logoutCSRF}}, cookies)
	require.Equal(t, http.StatusSeeOther, logout.Code)
	assert.Equal(t, "/ui/login", logout.Header().Get("Location"))
	cookies = mergeCookies(cookies, logout.Result().Cookies())

	again := env.do(t, http.MethodGet, "/ui/", nil, cookies)
	assert.Equal(t, http.StatusSeeOther, again.Code)
	assert.Equal(t, "/ui/login", again.Header().Get("Location"))
}

func TestUI_CSRFRejection(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")

	login := env.do(t, http.MethodGet, "/ui/login", nil, nil)
	csrf := extractCSRF(t, login.Body.String())
	cookies := login.Result().Cookies()

	posted := env.do(t, http.MethodPost, "/ui/login", url.Values{
		"csrf":    {csrf},
		"api_key": {"admin-secret"},
	}, cookies)
	require.Equal(t, http.StatusSeeOther, posted.Code)
	cookies = mergeCookies(cookies, posted.Result().Cookies())

	bad := env.do(t, http.MethodPost, "/ui/logout", url.Values{"csrf": {"not-the-token"}}, cookies)
	assert.Equal(t, http.StatusForbidden, bad.Code)
	assert.Contains(t, bad.Body.String(), "CSRF")
}

func TestUI_ScopeEnforcement(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	clock := &fixedClock{now: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	keys := app.NewAPIKeyService(env.repo, clock)
	created, err := keys.Create(app.WithPrincipal(context.Background(), app.BootstrapAdmin), app.CreateAPIKeyCommand{
		Name:   "worker-only",
		Scopes: []domain.Scope{domain.ScopeWorker},
	})
	require.NoError(t, err)

	login := env.do(t, http.MethodGet, "/ui/login", nil, nil)
	csrf := extractCSRF(t, login.Body.String())
	cookies := login.Result().Cookies()

	res := env.do(t, http.MethodPost, "/ui/login", url.Values{
		"csrf":    {csrf},
		"api_key": {created.Secret},
	}, cookies)
	assert.Equal(t, http.StatusForbidden, res.Code)
	assert.Contains(t, res.Body.String(), "dashboard")
}

func TestUI_DashboardScopeLogin(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	clock := &fixedClock{now: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	keys := app.NewAPIKeyService(env.repo, clock)
	created, err := keys.Create(app.WithPrincipal(context.Background(), app.BootstrapAdmin), app.CreateAPIKeyCommand{
		Name:   "dash",
		Scopes: []domain.Scope{domain.ScopeDashboardRead},
	})
	require.NoError(t, err)

	login := env.do(t, http.MethodGet, "/ui/login", nil, nil)
	csrf := extractCSRF(t, login.Body.String())
	cookies := login.Result().Cookies()

	posted := env.do(t, http.MethodPost, "/ui/login", url.Values{
		"csrf":    {csrf},
		"api_key": {created.Secret},
	}, cookies)
	require.Equal(t, http.StatusSeeOther, posted.Code)
	cookies = mergeCookies(cookies, posted.Result().Cookies())

	home := env.do(t, http.MethodGet, "/ui/", nil, cookies)
	require.Equal(t, http.StatusOK, home.Code)
	assert.Contains(t, home.Body.String(), "Overview")
	assert.Contains(t, home.Body.String(), "dash")
}

func TestUI_ErrorPages(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")
	res := env.do(t, http.MethodGet, "/ui/no-such-page", nil, nil)
	assert.Equal(t, http.StatusNotFound, res.Code)
	assert.Contains(t, res.Body.String(), "Page not found")
	assert.Contains(t, res.Header().Get("Content-Security-Policy"), "default-src 'self'")
}

func TestUI_LoginCSRFRequired(t *testing.T) {
	t.Parallel()
	env := newUITestEnv(t, "admin-secret")

	login := env.do(t, http.MethodGet, "/ui/login", nil, nil)
	cookies := login.Result().Cookies()

	res := env.do(t, http.MethodPost, "/ui/login", url.Values{
		"csrf":    {"wrong"},
		"api_key": {"admin-secret"},
	}, cookies)
	assert.Equal(t, http.StatusForbidden, res.Code)
	assert.Contains(t, res.Body.String(), "CSRF")
}

type uiTestEnv struct {
	handler http.Handler
	repo    *memory.Repository
}

func newUITestEnv(t *testing.T, bootstrap string) *uiTestEnv {
	t.Helper()
	repo := memory.New()
	clock := &fixedClock{now: time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: bootstrap}, log)
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ui, _, err := web.New(web.Options{
		Auth:          auth,
		Logger:        log,
		SessionKey:    key,
		SecureCookies: false,
		Clock:         clock.Now,
	})
	require.NoError(t, err)
	return &uiTestEnv{handler: ui.Handler(), repo: repo}
}

func (e *uiTestEnv) do(t *testing.T, method, path string, form url.Values, cookies []*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	return rec
}

func mergeCookies(existing, newer []*http.Cookie) []*http.Cookie {
	byName := make(map[string]*http.Cookie, len(existing)+len(newer))
	for _, c := range existing {
		byName[c.Name] = c
	}
	for _, c := range newer {
		if c.MaxAge < 0 {
			delete(byName, c.Name)
			continue
		}
		byName[c.Name] = c
	}
	out := make([]*http.Cookie, 0, len(byName))
	for _, c := range byName {
		out = append(out, c)
	}
	return out
}

func extractCSRF(t *testing.T, html string) string {
	t.Helper()
	const marker = `name="csrf" value="`
	i := strings.Index(html, marker)
	require.GreaterOrEqual(t, i, 0, "csrf field missing")
	rest := html[i+len(marker):]
	j := strings.Index(rest, `"`)
	require.GreaterOrEqual(t, j, 0)
	token := rest[:j]
	require.NotEmpty(t, token)
	return token
}

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }
