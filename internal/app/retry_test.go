package app_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/MEHRDADsmj/relay/internal/app"
)

func TestExponentialJitterPolicy_FullJitter(t *testing.T) {
	t.Parallel()
	p := app.ExponentialJitterPolicy{
		Base: time.Second,
		Max:  time.Minute,
		Rand: func() float64 { return 0.5 },
	}
	// attempt 1: 0.5 * min(1m, 1s * 2^0) = 0.5s
	assert.Equal(t, 500*time.Millisecond, p.NextDelay(1))
	// attempt 2: 0.5 * min(1m, 1s * 2) = 1s
	assert.Equal(t, time.Second, p.NextDelay(2))
	// attempt 3: 0.5 * 4s = 2s
	assert.Equal(t, 2*time.Second, p.NextDelay(3))
	// large attempt capped at Max
	assert.Equal(t, 30*time.Second, p.NextDelay(20))
}

func TestExponentialJitterPolicy_ZeroRand(t *testing.T) {
	t.Parallel()
	p := app.ExponentialJitterPolicy{
		Base: time.Second,
		Max:  time.Hour,
		Rand: func() float64 { return 0 },
	}
	assert.Equal(t, time.Duration(0), p.NextDelay(1))
}

func TestExponentialJitterPolicy_OverflowSafe(t *testing.T) {
	t.Parallel()
	p := app.ExponentialJitterPolicy{
		Base: time.Hour,
		Max:  24 * time.Hour,
		Rand: func() float64 { return math.Nextafter(1, 0) },
	}
	d := p.NextDelay(100)
	assert.True(t, d <= 24*time.Hour)
	assert.True(t, d > 0)
}
