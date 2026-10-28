package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfirm_YesFlagSkipsPrompt(t *testing.T) {
	stdinIsTTY = func() bool { return true }
	t.Cleanup(func() { stdinIsTTY = defaultStdinIsTTY })

	err := confirmDestructive("delete schedule", true, strings.NewReader(""), &bytes.Buffer{})
	require.NoError(t, err)
}

func TestConfirm_NonTTYWithoutYesErrors(t *testing.T) {
	stdinIsTTY = func() bool { return false }
	t.Cleanup(func() { stdinIsTTY = defaultStdinIsTTY })

	err := confirmDestructive("delete schedule", false, strings.NewReader(""), &bytes.Buffer{})
	require.Error(t, err)
	require.Equal(t, exitUsage, exitCode(err))
	require.Contains(t, err.Error(), "--yes")
}

func TestConfirm_TTYAcceptsYes(t *testing.T) {
	stdinIsTTY = func() bool { return true }
	t.Cleanup(func() { stdinIsTTY = defaultStdinIsTTY })

	var stderr bytes.Buffer
	err := confirmDestructive("revoke key", false, strings.NewReader("yes\n"), &stderr)
	require.NoError(t, err)
	require.Contains(t, stderr.String(), "revoke key")
}

func TestConfirm_TTYRejectsNo(t *testing.T) {
	stdinIsTTY = func() bool { return true }
	t.Cleanup(func() { stdinIsTTY = defaultStdinIsTTY })

	err := confirmDestructive("delete schedule", false, strings.NewReader("no\n"), &bytes.Buffer{})
	require.Error(t, err)
	require.Equal(t, exitUsage, exitCode(err))
}
