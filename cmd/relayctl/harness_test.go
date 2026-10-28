package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	relayhttp "github.com/MEHRDADsmj/relay/internal/adapter/http"
	"github.com/MEHRDADsmj/relay/internal/adapter/storage/memory"
	"github.com/MEHRDADsmj/relay/internal/app"
)

const testBootstrapKey = "relayctl-test-bootstrap"

var updateGoldens = flag.Bool("update", false, "update golden files")

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type testEnv struct {
	Server *httptest.Server
	URL    string
	APIKey string
}

func startTestServer(t *testing.T) testEnv {
	t.Helper()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	repo := memory.New()
	clock := fixedClock{t: now}
	log := discardLog()
	jobs := app.NewJobService(repo, clock, app.UUIDv7Generator{}, app.FixedDelayRetryPolicy{Delay: time.Second}, log, repo)
	queues := app.NewQueueService(repo, repo, repo, clock)
	schedules := app.NewScheduleService(repo, jobs, adaptercron.NewParser(), clock, app.UUIDv7Generator{})
	apiKeys := app.NewAPIKeyService(repo, clock)
	auth := app.NewAuthService(repo, clock, app.AuthServiceConfig{BootstrapKey: testBootstrapKey}, log)
	h := relayhttp.NewRouter(jobs, queues, schedules, apiKeys, relayhttp.Options{
		APIKey: testBootstrapKey,
		Auth:   auth,
		Logger: log,
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return testEnv{Server: srv, URL: srv.URL, APIKey: testBootstrapKey}
}

type runResult struct {
	Stdout string
	Stderr string
	Code   int
}

func runCLI(t *testing.T, env testEnv, stdin string, args ...string) runResult {
	t.Helper()
	stdinIsTTY = func() bool { return false }
	t.Cleanup(func() { stdinIsTTY = defaultStdinIsTTY })

	var stdout, stderr bytes.Buffer
	root := newRoot()
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetIn(strings.NewReader(stdin))
	full := append([]string{
		"--server", env.URL,
		"--api-key", env.APIKey,
		"--timeout", "5s",
	}, args...)
	root.SetArgs(full)
	err := root.ExecuteContext(context.Background())
	if err != nil {
		fmt.Fprintln(&stderr, err.Error())
	}
	return runResult{Stdout: stdout.String(), Stderr: stderr.String(), Code: exitCode(err)}
}

func goldenPath(name string) string {
	return filepath.Join("testdata", name+".golden")
}

var (
	reUUID    = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reRFC3339 = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
	reSecret  = regexp.MustCompile(`rly_[A-Za-z0-9]+_[A-Za-z0-9]+`)
	rePrefix  = regexp.MustCompile(`"prefix":\s*"[^"]+"`)
)

func scrub(s string) string {
	s = reSecret.ReplaceAllString(s, "rly_PREFIX_SECRET")
	s = reUUID.ReplaceAllString(s, "00000000-0000-0000-0000-000000000000")
	s = reRFC3339.ReplaceAllString(s, "2026-10-28T12:00:00Z")
	s = rePrefix.ReplaceAllString(s, `"prefix": "PREFIX12"`)
	// KV table row: "Prefix     abcd1234"
	s = regexp.MustCompile(`(?m)^(Prefix\s+)\S+`).ReplaceAllString(s, `${1}PREFIX12`)
	// columnar table PREFIX field (8-char token between tabs)
	s = regexp.MustCompile(`(?m)^([0-9a-fA-F-]{36}\t[^\t]+\t)([A-Za-z0-9]{8})(\t)`).ReplaceAllString(s, `${1}PREFIX12${3}`)
	return s
}

func assertGolden(t *testing.T, name, got string) {
	t.Helper()
	got = scrub(got)
	path := goldenPath(name)
	if *updateGoldens {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
		return
	}
	want, err := os.ReadFile(path) //nolint:gosec // fixed testdata golden path
	require.NoError(t, err, "missing golden %s; rerun with -update", path)
	require.Equal(t, string(want), got)
}
