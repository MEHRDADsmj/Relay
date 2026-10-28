package app

import "time"

// FixedDelayRetryPolicy always returns the same delay regardless of attempt.
type FixedDelayRetryPolicy struct {
	Delay time.Duration
}

// NextDelay implements RetryPolicy.
func (p FixedDelayRetryPolicy) NextDelay(attempt int) time.Duration {
	_ = attempt
	return p.Delay
}
