package app

import (
	"math"
	"time"
)

// FixedDelayRetryPolicy always returns the same delay regardless of attempt.
type FixedDelayRetryPolicy struct {
	Delay time.Duration
}

// NextDelay implements RetryPolicy.
func (p FixedDelayRetryPolicy) NextDelay(attempt int) time.Duration {
	_ = attempt
	return p.Delay
}

// ExponentialJitterPolicy implements full-jitter exponential backoff:
//
//	delay = rand[0,1) * min(Max, Base * 2^(attempt-1))
//
// Rand must return a value in [0,1). Inject a fixed Rand for deterministic tests.
type ExponentialJitterPolicy struct {
	Base time.Duration
	Max  time.Duration
	Rand func() float64
}

const maxBackoffExponent = 62 // prevent shift overflow on int64

// NextDelay implements RetryPolicy.
func (p ExponentialJitterPolicy) NextDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := p.Base
	if base <= 0 {
		base = time.Second
	}
	max := p.Max
	if max <= 0 {
		max = time.Hour
	}
	exp := attempt - 1
	if exp > maxBackoffExponent {
		exp = maxBackoffExponent
	}
	// Cap before multiply: Base * 2^exp may overflow Duration.
	ceiling := base
	if exp > 0 {
		shift := base << exp
		if shift/base != 1<<uint(exp) || shift < 0 {
			ceiling = max
		} else {
			ceiling = shift
		}
	}
	if ceiling > max {
		ceiling = max
	}
	r := 0.0
	if p.Rand != nil {
		r = p.Rand()
	}
	if r < 0 {
		r = 0
	}
	if r >= 1 {
		r = math.Nextafter(1, 0)
	}
	return time.Duration(float64(ceiling) * r)
}
