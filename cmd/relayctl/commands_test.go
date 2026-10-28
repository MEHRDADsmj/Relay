package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersion(t *testing.T) {
	env := startTestServer(t)
	res := runCLI(t, env, "", "version")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	require.Contains(t, res.Stdout, "relayctl")
	require.Contains(t, res.Stdout, "dev")
}

func TestJobEnqueueGetAttemptsCancelRetryList_TableAndJSON(t *testing.T) {
	env := startTestServer(t)

	res := runCLI(t, env, "", "job", "enqueue",
		"--queue", "default", "--type", "demo.job",
		"--payload", `{"n":1}`, "--priority", "5",
		"--max-attempts", "3", "--idempotency-key", "k1",
	)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "job_enqueue_table", res.Stdout)

	res = runCLI(t, env, "", "-o", "json", "job", "enqueue",
		"--queue", "default", "--type", "demo.job",
		"--payload", `{"n":2}`, "--idempotency-key", "k2",
	)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	id := jsonField(t, res.Stdout, "id")

	res = runCLI(t, env, "", "job", "get", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "job_get_table", res.Stdout)

	res = runCLI(t, env, "", "-o", "json", "job", "get", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "job_get_json", res.Stdout)

	res = runCLI(t, env, "", "job", "attempts", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "job_attempts_table", res.Stdout)

	res = runCLI(t, env, "", "job", "cancel", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)

	res = runCLI(t, env, "", "job", "retry", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)

	res = runCLI(t, env, "", "job", "list", "--queue", "default", "--status", "pending", "--limit", "10")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "job_list_table", res.Stdout)

	res = runCLI(t, env, "", "-o", "json", "job", "list", "--queue", "default", "--status", "pending", "--all")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "job_list_json", res.Stdout)
}

func TestJobGet_NotFound(t *testing.T) {
	env := startTestServer(t)
	res := runCLI(t, env, "", "job", "get", "00000000-0000-7000-8000-000000000099")
	require.Equal(t, exitNotFound, res.Code, res.Stderr)
}

func TestQueueCommands(t *testing.T) {
	env := startTestServer(t)

	require.Equal(t, exitOK, runCLI(t, env, "", "job", "enqueue",
		"--queue", "q1", "--type", "t", "--payload", `{}`,
	).Code)

	res := runCLI(t, env, "", "queue", "list")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "queue_list_table", res.Stdout)

	res = runCLI(t, env, "", "-o", "json", "queue", "list")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "queue_list_json", res.Stdout)

	res = runCLI(t, env, "", "queue", "stats", "q1")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "queue_stats_table", res.Stdout)

	res = runCLI(t, env, "", "queue", "pause", "q1")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	res = runCLI(t, env, "", "queue", "resume", "q1")
	require.Equal(t, exitOK, res.Code, res.Stderr)

	res = runCLI(t, env, "", "queue", "retry-dead", "q1", "--limit", "5")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "queue_retry_dead_table", res.Stdout)
}

func TestScheduleCommands(t *testing.T) {
	env := startTestServer(t)

	res := runCLI(t, env, "", "schedule", "create",
		"--name", "hourly-demo",
		"--cron", "@hourly",
		"--timezone", "UTC",
		"--queue", "default",
		"--type", "demo.job",
		"--payload", `{"from":"sched"}`,
	)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "schedule_create_table", res.Stdout)

	res = runCLI(t, env, "", "-o", "json", "schedule", "get", "hourly-demo")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "schedule_get_json", res.Stdout)
	id := jsonField(t, res.Stdout, "id")

	res = runCLI(t, env, "", "schedule", "list")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "schedule_list_table", res.Stdout)

	res = runCLI(t, env, "", "schedule", "disable", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	res = runCLI(t, env, "", "schedule", "enable", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)

	res = runCLI(t, env, "", "schedule", "edit", id, "--cron", "@daily")
	require.Equal(t, exitOK, res.Code, res.Stderr)

	res = runCLI(t, env, "", "schedule", "trigger", id)
	require.Equal(t, exitOK, res.Code, res.Stderr)
	require.Contains(t, scrub(res.Stdout), "00000000-0000-0000-0000-000000000000")

	res = runCLI(t, env, "", "schedule", "delete", id) // no --yes, non-TTY
	require.Equal(t, exitUsage, res.Code)
	require.Contains(t, res.Stderr+res.Stdout, "--yes")

	res = runCLI(t, env, "", "schedule", "delete", id, "--yes")
	require.Equal(t, exitOK, res.Code, res.Stderr)
}

func TestAPIKeyCommands(t *testing.T) {
	env := startTestServer(t)

	res := runCLI(t, env, "", "apikey", "create", "--name", "prod", "--scopes", "producer,worker")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "apikey_create_table", res.Stdout)
	require.Contains(t, scrub(res.Stdout), "rly_PREFIX_SECRET")

	res = runCLI(t, env, "", "-o", "json", "apikey", "list")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	assertGolden(t, "apikey_list_json", res.Stdout)
	id := jsonArrayField(t, res.Stdout, 0, "id")

	res = runCLI(t, env, "", "apikey", "revoke", id)
	require.Equal(t, exitUsage, res.Code)

	res = runCLI(t, env, "", "apikey", "revoke", id, "--yes")
	require.Equal(t, exitOK, res.Code, res.Stderr)
}

func TestUsageError_MissingRequired(t *testing.T) {
	env := startTestServer(t)
	res := runCLI(t, env, "", "job", "enqueue")
	require.Equal(t, exitUsage, res.Code)
}

func TestCompletionCommandExists(t *testing.T) {
	env := startTestServer(t)
	res := runCLI(t, env, "", "completion", "bash")
	require.Equal(t, exitOK, res.Code, res.Stderr)
	require.Contains(t, res.Stdout, "bash")
}

func TestConfirmViaCLI_TTYYes(t *testing.T) {
	env := startTestServer(t)
	create := runCLI(t, env, "", "-o", "json", "schedule", "create",
		"--name", "to-delete", "--cron", "@hourly",
		"--queue", "default", "--type", "t", "--payload", `{}`,
	)
	require.Equal(t, exitOK, create.Code, create.Stderr)
	id := jsonField(t, create.Stdout, "id")

	stdinIsTTY = func() bool { return true }
	t.Cleanup(func() { stdinIsTTY = defaultStdinIsTTY })

	var stdout, stderr strings.Builder
	root := newRoot()
	root.SetOut(&stdout)
	root.SetErr(&stderr)
	root.SetIn(strings.NewReader("yes\n"))
	root.SetArgs([]string{
		"--server", env.URL, "--api-key", env.APIKey,
		"schedule", "delete", id,
	})
	err := root.Execute()
	require.Equal(t, exitOK, exitCode(err), stderr.String()+" "+errString(err))
}

func jsonField(t *testing.T, raw, key string) string {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &m))
	v, ok := m[key].(string)
	require.True(t, ok && v != "", "missing %s in %s", key, raw)
	return v
}

func jsonArrayField(t *testing.T, raw string, idx int, key string) string {
	t.Helper()
	var arr []map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &arr))
	require.Greater(t, len(arr), idx)
	v, ok := arr[idx][key].(string)
	require.True(t, ok && v != "", "missing %s", key)
	return v
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
