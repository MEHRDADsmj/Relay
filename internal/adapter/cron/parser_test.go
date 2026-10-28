package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adaptercron "github.com/MEHRDADsmj/relay/internal/adapter/cron"
	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestParser_Validate(t *testing.T) {
	t.Parallel()
	p := adaptercron.NewParser()
	var _ app.CronParser = p
	var _ domain.CronParser = p

	require.NoError(t, p.Validate("0 * * * *"))
	require.NoError(t, p.Validate("@hourly"))
	require.NoError(t, p.Validate("@every 5m"))
	require.Error(t, p.Validate(""))
	require.Error(t, p.Validate("not a cron"))
	require.Error(t, p.Validate("* * *"))
}

func TestParser_Next_Hourly(t *testing.T) {
	t.Parallel()
	p := adaptercron.NewParser()
	loc := time.UTC
	after := time.Date(2026, 6, 1, 10, 15, 0, 0, loc)
	next, err := p.Next("@hourly", loc, after)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 6, 1, 11, 0, 0, 0, loc), next)
}

func TestParser_Next_StrictlyAfter(t *testing.T) {
	t.Parallel()
	p := adaptercron.NewParser()
	loc := time.UTC
	onBoundary := time.Date(2026, 6, 1, 11, 0, 0, 0, loc)
	next, err := p.Next("0 * * * *", loc, onBoundary)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 6, 1, 12, 0, 0, 0, loc), next)
}

func TestParser_Next_Timezone(t *testing.T) {
	t.Parallel()
	p := adaptercron.NewParser()
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	after := time.Date(2026, 6, 1, 10, 30, 0, 0, loc)
	next, err := p.Next("0 11 * * *", loc, after)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 6, 1, 11, 0, 0, 0, loc), next)
}
