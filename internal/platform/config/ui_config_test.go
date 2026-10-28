package config

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoad_UISessionKeyProductionRequired(t *testing.T) {
	t.Setenv("RELAY_POSTGRES_URL", "postgres://relay:relay@localhost:54329/relay?sslmode=disable")
	clearOptional(t)
	t.Setenv("RELAY_ENVIRONMENT", "production")

	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "RELAY_UI_SESSION_KEY")
}

func TestLoad_UISessionKeyAndSecureCookies(t *testing.T) {
	t.Setenv("RELAY_POSTGRES_URL", "postgres://relay:relay@localhost:54329/relay?sslmode=disable")
	clearOptional(t)
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	t.Setenv("RELAY_ENVIRONMENT", "production")
	t.Setenv("RELAY_UI_SESSION_KEY", base64.StdEncoding.EncodeToString(key))
	t.Setenv("RELAY_UI_SECURE_COOKIES", "false")
	t.Setenv("RELAY_UI_TRACE_URL_TEMPLATE", "http://localhost:16686/trace/{trace_id}")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, key, cfg.UISessionKey)
	require.False(t, cfg.UISecureCookies)
	require.Equal(t, "http://localhost:16686/trace/{trace_id}", cfg.UITraceURLTemplate)
	require.True(t, cfg.UIEnabled)
}

func TestLoad_UIDisabledSkipsSessionKeyInProd(t *testing.T) {
	t.Setenv("RELAY_POSTGRES_URL", "postgres://relay:relay@localhost:54329/relay?sslmode=disable")
	clearOptional(t)
	t.Setenv("RELAY_ENVIRONMENT", "production")
	t.Setenv("RELAY_UI_ENABLED", "false")

	cfg, err := Load()
	require.NoError(t, err)
	require.False(t, cfg.UIEnabled)
}
