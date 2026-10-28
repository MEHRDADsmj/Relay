package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExitCode(t *testing.T) {
	require.Equal(t, exitOK, exitCode(nil))
	require.Equal(t, exitUsage, exitCode(usageErrorf("bad flag")))
	require.Equal(t, exitAPI, exitCode(apiErrorf("boom")))
	require.Equal(t, exitNotFound, exitCode(notFoundErrorf("missing")))
	require.Equal(t, exitAPI, exitCode(errors.New("plain")))
}
