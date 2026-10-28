package logging

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew_JSONInfo(t *testing.T) {
	var buf bytes.Buffer
	log, err := NewWithWriter("info", "json", &buf)
	require.NoError(t, err)

	log.Info("hello", "k", "v")
	require.Contains(t, buf.String(), `"msg":"hello"`)
	require.Contains(t, buf.String(), `"k":"v"`)
}

func TestNew_TextDebug(t *testing.T) {
	var buf bytes.Buffer
	log, err := NewWithWriter("debug", "text", &buf)
	require.NoError(t, err)

	log.Debug("dbg")
	require.Contains(t, buf.String(), "dbg")
}

func TestNew_InvalidLevel(t *testing.T) {
	_, err := New("trace", "json")
	require.Error(t, err)
}

func TestNew_InvalidFormat(t *testing.T) {
	_, err := New("info", "xml")
	require.Error(t, err)
}

func TestNew_WarnFiltersDebug(t *testing.T) {
	var buf bytes.Buffer
	log, err := NewWithWriter("warn", "text", &buf)
	require.NoError(t, err)

	log.Debug("hidden")
	log.Warn("visible")
	require.NotContains(t, buf.String(), "hidden")
	require.Contains(t, buf.String(), "visible")
}
