package web

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/a-h/templ"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/adapter/web/templates"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

type ctxKey int

const sessionCtxKey ctxKey = 1

// Options configures the dashboard adapter.
type Options struct {
	Auth          *app.AuthService
	Queues        *app.QueueService
	Jobs          *app.JobService
	Schedules     *app.ScheduleService
	CachedStats   *app.CachedQueueStats
	Leadership    *app.Leadership
	InstanceID    string
	TraceURLTpl   string // e.g. http://localhost:16686/trace/{trace_id}
	Logger        *slog.Logger
	SessionKey    []byte // 32 bytes; generated if empty
	SecureCookies bool
	Clock         func() time.Time // optional; defaults to time.Now
	SSEInterval   time.Duration    // default 2s
}

// Server is the /ui dashboard HTTP adapter.
type Server struct {
	auth        *app.AuthService
	queues      *app.QueueService
	jobs        *app.JobService
	schedules   *app.ScheduleService
	cached      *app.CachedQueueStats
	leadership  *app.Leadership
	instanceID  string
	traceURLTpl string
	log         *slog.Logger
	sessions    *sessionStore
	loginRL     *loginLimiter
	sseInterval time.Duration
	handler     http.Handler
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
	sse := opts.SSEInterval
	if sse <= 0 {
		sse = 2 * time.Second
	}
	s := &Server{
		auth:        opts.Auth,
		queues:      opts.Queues,
		jobs:        opts.Jobs,
		schedules:   opts.Schedules,
		cached:      opts.CachedStats,
		leadership:  opts.Leadership,
		instanceID:  opts.InstanceID,
		traceURLTpl: opts.TraceURLTpl,
		log:         log,
		sessions:    store,
		loginRL:     newLoginLimiter(),
		sseInterval: sse,
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

	r.Group(func(r chi.Router) {
		r.Use(s.requireSession)
		r.Get("/ui/", s.handleHome)
		r.Get("/ui", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusSeeOther)
		})
		r.Get("/ui/events", s.handleSSE)
		r.Get("/ui/queues/{queue}", s.handleQueue)
		r.Get("/ui/jobs/{id}", s.handleJob)
		r.Get("/ui/schedules", s.handleSchedules)
		r.Get("/ui/schedules/{id}", s.handleScheduleDetail)

		r.Group(func(r chi.Router) {
			r.Use(s.requireCSRF)
			r.Post("/ui/queues/{queue}/pause", s.handleQueuePause)
			r.Post("/ui/queues/{queue}/resume", s.handleQueueResume)
			r.Post("/ui/queues/{queue}/retry-dead", s.handleQueueRetryDead)
			r.Post("/ui/jobs/{id}/cancel", s.handleJobCancel)
			r.Post("/ui/jobs/{id}/retry", s.handleJobRetry)
			r.Post("/ui/schedules/{id}/enable", s.handleScheduleEnable)
			r.Post("/ui/schedules/{id}/disable", s.handleScheduleDisable)
			r.Post("/ui/schedules/{id}/trigger", s.handleScheduleTrigger)
		})
	})

	r.NotFound(s.handleNotFound)
	return r
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildOverview(r.Context())
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not load overview.")
		return
	}
	s.render(w, r, http.StatusOK, templates.HomePage(data))
}

func (s *Server) overviewMeta(ctx context.Context) templates.OverviewData {
	sess := sessionFrom(ctx)
	p, _ := app.PrincipalFrom(ctx)
	name := p.Name
	if name == "" {
		name = sess.KeyID
	}
	inst, holding := "", false
	if s.leadership != nil {
		inst, holding = s.leadership.Snapshot()
	}
	if inst == "" {
		inst = s.instanceID
	}
	return templates.OverviewData{
		CSRF:       sess.CSRF,
		KeyName:    name,
		InstanceID: inst,
		IsLeader:   holding,
	}
}

func (s *Server) buildOverview(ctx context.Context) (templates.OverviewData, error) {
	data := s.overviewMeta(ctx)
	if s.queues == nil {
		return data, nil
	}
	queues, err := s.queues.ListQueues(ctx)
	if err != nil {
		return data, err
	}
	rows := make([]templates.QueueRow, 0, len(queues))
	for _, q := range queues {
		st, err := s.queues.Stats(ctx, q.Name)
		if err != nil {
			return data, err
		}
		data.Totals.Pending += st.Pending
		data.Totals.Scheduled += st.Scheduled
		data.Totals.Running += st.Running
		data.Totals.Dead += st.Dead
		spark := ""
		if pts, err := s.queues.Throughput(ctx, q.Name, app.ThroughputWindow15m); err == nil {
			spark = templates.SparklineSVG(pts, 120, 28)
		}
		rows = append(rows, templates.QueueRow{Stats: st, Sparkline: spark})
	}
	data.Queues = rows
	return data, nil
}

// buildOverviewSSE uses CachedQueueStats for the live table (plan §3.2).
func (s *Server) buildOverviewSSE(ctx context.Context) (templates.OverviewData, error) {
	data := s.overviewMeta(ctx)
	if s.cached != nil {
		stats, err := s.cached.Snapshot(ctx)
		if err != nil {
			return data, err
		}
		rows := make([]templates.QueueRow, 0, len(stats))
		for _, st := range stats {
			data.Totals.Pending += st.Pending
			data.Totals.Scheduled += st.Scheduled
			data.Totals.Running += st.Running
			data.Totals.Dead += st.Dead
			spark := ""
			if s.queues != nil {
				if pts, err := s.queues.Throughput(ctx, st.Queue, app.ThroughputWindow15m); err == nil {
					spark = templates.SparklineSVG(pts, 120, 28)
				}
			}
			rows = append(rows, templates.QueueRow{Stats: st, Sparkline: spark})
		}
		data.Queues = rows
		return data, nil
	}
	return s.buildOverview(ctx)
}

func (s *Server) handleQueue(w http.ResponseWriter, r *http.Request) {
	queue := chi.URLParam(r, "queue")
	if err := domain.ValidateQueue(queue); err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid queue", err.Error())
		return
	}
	data, err := s.buildQueueDetail(r.Context(), queue, r)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			s.renderError(w, r, http.StatusNotFound, "Queue not found", "That queue does not exist.")
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not load queue.")
		return
	}
	if r.URL.Query().Get("fragment") == "jobs" {
		s.render(w, r, http.StatusOK, templates.QueueJobsFragment(data))
		return
	}
	s.render(w, r, http.StatusOK, templates.QueuePage(data))
}

func (s *Server) buildQueueDetail(ctx context.Context, queue string, r *http.Request) (templates.QueueDetailData, error) {
	sess := sessionFrom(ctx)
	p, _ := app.PrincipalFrom(ctx)
	window := r.URL.Query().Get("window")
	if window == "" {
		window = "15m"
	}
	var winDur time.Duration
	switch window {
	case "1h":
		winDur = app.ThroughputWindow1h
	case "24h":
		winDur = app.ThroughputWindow24h
	default:
		window = "15m"
		winDur = app.ThroughputWindow15m
	}
	status := domain.Status(r.URL.Query().Get("status"))
	switch status {
	case domain.StatusPending, domain.StatusRunning, domain.StatusSucceeded, domain.StatusDead, domain.StatusCancelled:
	default:
		status = domain.StatusPending
	}
	st, err := s.queues.Stats(ctx, queue)
	if err != nil {
		return templates.QueueDetailData{}, err
	}
	chart := ""
	if pts, err := s.queues.Throughput(ctx, queue, winDur); err == nil {
		chart = templates.ThroughputChartSVG(pts)
	}
	var jobs []domain.Job
	var next string
	if s.jobs != nil {
		page, err := s.jobs.ListByStatus(ctx, app.ListByStatusCommand{
			Queue:  queue,
			Status: status,
			Page:   domain.PageRequest{Limit: 25, Cursor: r.URL.Query().Get("cursor")},
		})
		if err != nil {
			return templates.QueueDetailData{}, err
		}
		jobs = page.Items
		next = page.NextCursor
	}
	return templates.QueueDetailData{
		CSRF:       sess.CSRF,
		Queue:      queue,
		Stats:      st,
		Window:     window,
		ChartSVG:   chart,
		Status:     status,
		Jobs:       jobs,
		NextCursor: next,
		CanAdmin:   p.Can(domain.ScopeAdmin),
	}, nil
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid job", "Job id must be a UUID.")
		return
	}
	if s.jobs == nil {
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Jobs service not configured.")
		return
	}
	job, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			s.renderError(w, r, http.StatusNotFound, "Job not found", "That job does not exist.")
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not load job.")
		return
	}
	attempts, _ := s.jobs.ListAttempts(r.Context(), id)
	sess := sessionFrom(r.Context())
	p, _ := app.PrincipalFrom(r.Context())
	canMutate := p.Can(domain.ScopeProducer) || p.Can(domain.ScopeAdmin)
	canCancel := canMutate && (job.Status == domain.StatusPending || job.Status == domain.StatusRunning)
	canRetry := canMutate && (job.Status == domain.StatusDead || job.Status == domain.StatusCancelled)
	traceURL := ""
	if s.traceURLTpl != "" {
		if tid := job.TraceContext["traceparent"]; tid != "" {
			// W3C: version-traceid-spanid-flags; extract 32-hex trace id.
			parts := strings.Split(tid, "-")
			if len(parts) >= 2 {
				traceURL = strings.ReplaceAll(s.traceURLTpl, "{trace_id}", parts[1])
			}
		}
	}
	s.render(w, r, http.StatusOK, templates.JobPage(templates.JobDetailData{
		CSRF:      sess.CSRF,
		Job:       job,
		Payload:   templates.PrettyJSON(job.Payload),
		Attempts:  attempts,
		TraceURL:  traceURL,
		CanMutate: canMutate,
		CanCancel: canCancel,
		CanRetry:  canRetry,
	}))
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	if s.schedules == nil {
		s.render(w, r, http.StatusOK, templates.SchedulesPage(templates.SchedulesData{
			CSRF: sessionFrom(r.Context()).CSRF,
		}))
		return
	}
	page, err := s.schedules.List(r.Context(), domain.PageRequest{Limit: 100})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not load schedules.")
		return
	}
	sess := sessionFrom(r.Context())
	p, _ := app.PrincipalFrom(r.Context())
	rows := make([]templates.ScheduleRow, 0, len(page.Items))
	for _, sch := range page.Items {
		rows = append(rows, templates.ScheduleRow{
			Schedule: sch,
			CronDesc: templates.DescribeCron(sch.Cron),
		})
	}
	s.render(w, r, http.StatusOK, templates.SchedulesPage(templates.SchedulesData{
		CSRF:      sess.CSRF,
		Schedules: rows,
		CanAdmin:  p.Can(domain.ScopeAdmin),
	}))
}

func (s *Server) handleScheduleDetail(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid schedule", "Schedule id must be a UUID.")
		return
	}
	if s.schedules == nil {
		s.renderError(w, r, http.StatusNotFound, "Schedule not found", "Schedules are not configured.")
		return
	}
	sch, err := s.schedules.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			s.renderError(w, r, http.StatusNotFound, "Schedule not found", "That schedule does not exist.")
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Could not load schedule.")
		return
	}
	var jobs []domain.Job
	if s.jobs != nil {
		jobs, _ = s.jobs.ListJobsBySchedule(r.Context(), id, 50)
	}
	sess := sessionFrom(r.Context())
	p, _ := app.PrincipalFrom(r.Context())
	s.render(w, r, http.StatusOK, templates.ScheduleDetailPage(templates.ScheduleDetailData{
		CSRF:     sess.CSRF,
		Schedule: sch,
		CronDesc: templates.DescribeCron(sch.Cron),
		Jobs:     jobs,
		CanAdmin: p.Can(domain.ScopeAdmin),
	}))
}

func (s *Server) handleQueuePause(w http.ResponseWriter, r *http.Request) {
	s.mutateQueue(w, r, true)
}

func (s *Server) handleQueueResume(w http.ResponseWriter, r *http.Request) {
	s.mutateQueue(w, r, false)
}

func (s *Server) mutateQueue(w http.ResponseWriter, r *http.Request, pause bool) {
	queue := chi.URLParam(r, "queue")
	var err error
	if pause {
		_, err = s.queues.Pause(r.Context(), queue)
	} else {
		_, err = s.queues.Resume(r.Context(), queue)
	}
	if err != nil {
		s.mapMutateErr(w, r, err)
		return
	}
	if s.cached != nil {
		s.cached.Invalidate()
	}
	http.Redirect(w, r, "/ui/queues/"+queue, http.StatusSeeOther)
}

func (s *Server) handleQueueRetryDead(w http.ResponseWriter, r *http.Request) {
	queue := chi.URLParam(r, "queue")
	_, err := s.queues.RetryAllDead(r.Context(), app.RetryAllDeadCommand{Queue: queue})
	if err != nil {
		s.mapMutateErr(w, r, err)
		return
	}
	if s.cached != nil {
		s.cached.Invalidate()
	}
	if isHTMX(r) {
		data, err := s.buildQueueDetail(r.Context(), queue, r)
		if err != nil {
			s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "Retried, but could not refresh list.")
			return
		}
		s.render(w, r, http.StatusOK, templates.QueueJobsFragment(data))
		return
	}
	http.Redirect(w, r, "/ui/queues/"+queue+"?status=dead", http.StatusSeeOther)
}

func (s *Server) handleJobCancel(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid job", "Job id must be a UUID.")
		return
	}
	if _, err := s.jobs.Cancel(r.Context(), id); err != nil {
		s.mapMutateErr(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id.String(), http.StatusSeeOther)
}

func (s *Server) handleJobRetry(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid job", "Job id must be a UUID.")
		return
	}
	if _, err := s.jobs.Retry(r.Context(), id); err != nil {
		s.mapMutateErr(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+id.String(), http.StatusSeeOther)
}

func (s *Server) handleScheduleEnable(w http.ResponseWriter, r *http.Request) {
	s.toggleSchedule(w, r, true)
}

func (s *Server) handleScheduleDisable(w http.ResponseWriter, r *http.Request) {
	s.toggleSchedule(w, r, false)
}

func (s *Server) toggleSchedule(w http.ResponseWriter, r *http.Request, enable bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid schedule", "Schedule id must be a UUID.")
		return
	}
	if enable {
		_, err = s.schedules.Enable(r.Context(), id)
	} else {
		_, err = s.schedules.Disable(r.Context(), id)
	}
	if err != nil {
		s.mapMutateErr(w, r, err)
		return
	}
	http.Redirect(w, r, refererOr(r, "/ui/schedules/"+id.String()), http.StatusSeeOther)
}

func (s *Server) handleScheduleTrigger(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Invalid schedule", "Schedule id must be a UUID.")
		return
	}
	job, err := s.schedules.TriggerNow(r.Context(), id)
	if err != nil {
		s.mapMutateErr(w, r, err)
		return
	}
	http.Redirect(w, r, "/ui/jobs/"+job.ID.String(), http.StatusSeeOther)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ticker := time.NewTicker(s.sseInterval)
	defer ticker.Stop()

	ctx := r.Context()
	if !s.writeSSEOverview(ctx, w, flusher) {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.writeSSEOverview(ctx, w, flusher) {
				return
			}
		}
	}
}

func (s *Server) writeSSEOverview(ctx context.Context, w http.ResponseWriter, flusher http.Flusher) bool {
	data, err := s.buildOverviewSSE(ctx)
	if err != nil {
		s.log.ErrorContext(ctx, "sse overview", "error", err)
		return true
	}
	var buf strings.Builder
	if err := templates.OverviewLive(data).Render(ctx, &buf); err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "event: overview\ndata: %s\n\n", flattenSSE(buf.String())); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

func flattenSSE(html string) string {
	return strings.ReplaceAll(html, "\n", "")
}

func (s *Server) mapMutateErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrForbidden), errors.Is(err, domain.ErrUnauthenticated):
		s.renderError(w, r, http.StatusForbidden, "Forbidden", "You do not have permission for this action.")
	case errors.Is(err, domain.ErrNotFound):
		s.renderError(w, r, http.StatusNotFound, "Not found", "The requested resource was not found.")
	case errors.Is(err, domain.ErrInvalidTransition):
		s.renderError(w, r, http.StatusConflict, "Conflict", err.Error())
	default:
		var ve *domain.ValidationError
		if errors.As(err, &ve) {
			s.renderError(w, r, http.StatusBadRequest, "Invalid request", ve.Error())
			return
		}
		s.log.ErrorContext(r.Context(), "ui mutate", "error", err, "path", r.URL.Path)
		s.renderError(w, r, http.StatusInternalServerError, "Something went wrong", "The action could not be completed.")
	}
}

func isHTMX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

func refererOr(r *http.Request, fallback string) string {
	if ref := r.Header.Get("Referer"); strings.HasPrefix(ref, "/ui") || strings.Contains(ref, "/ui/") {
		return ref
	}
	return fallback
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
