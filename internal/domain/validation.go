package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

const (
	// MaxPayloadBytes is the maximum accepted job payload size (256 KiB).
	MaxPayloadBytes = 256 << 10
	// MaxErrorMessageBytes is the maximum stored failure message size.
	MaxErrorMessageBytes = 4096
	// MaxIdempotencyKeyLen is the maximum idempotency key length in runes/bytes (ASCII).
	MaxIdempotencyKeyLen = 255
	// MaxWorkerIDLen is the maximum worker ID length.
	MaxWorkerIDLen = 128

	DefaultPriority      = 0
	DefaultMaxAttempts   = 10
	DefaultLeaseDuration = 30 * time.Second
	DefaultMaxJobs       = 1

	MinPriority      = -100
	MaxPriority      = 100
	MinMaxAttempts   = 1
	MaxMaxAttempts   = 100
	MinLeaseDuration = 5 * time.Second
	MaxLeaseDuration = 15 * time.Minute
	MinMaxJobs       = 1
	MaxMaxJobs       = 100

	DefaultRetryAllDeadLimit = 1000
	MinRetryAllDeadLimit     = 1
	MaxRetryAllDeadLimit     = 10_000
)

// MaxLeaseWait is the maximum long-poll duration for leases.
// Default 30s; relay-server overrides from RELAY_LEASE_MAX_WAIT at startup.
var MaxLeaseWait = 30 * time.Second

var (
	queuePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	typePattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)
)

// ValidateQueue checks the queue name against the domain regex.
func ValidateQueue(queue string) error {
	if !queuePattern.MatchString(queue) {
		return validationErrorf("queue", "must match ^[a-z0-9][a-z0-9_-]{0,63}$")
	}
	return nil
}

// ValidateType checks the job type against the domain regex.
func ValidateType(typ string) error {
	if !typePattern.MatchString(typ) {
		return validationErrorf("type", "must match ^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
	}
	return nil
}

// NormalizePayload defaults nil/empty payloads to {} and validates JSON + size.
func NormalizePayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return []byte("{}"), nil
	}
	if len(payload) > MaxPayloadBytes {
		return nil, validationErrorf("payload", "must be at most 256 KiB")
	}
	if !json.Valid(payload) {
		return nil, validationErrorf("payload", "must be valid JSON")
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, nil
}

// NormalizePriority defaults a nil priority to 0 and validates the range.
func NormalizePriority(priority *int) (int, error) {
	if priority == nil {
		return DefaultPriority, nil
	}
	if *priority < MinPriority || *priority > MaxPriority {
		return 0, validationErrorf("priority", "must be between -100 and 100")
	}
	return *priority, nil
}

// NormalizeMaxAttempts defaults a nil value to 10 and validates the range.
func NormalizeMaxAttempts(maxAttempts *int) (int, error) {
	if maxAttempts == nil {
		return DefaultMaxAttempts, nil
	}
	if *maxAttempts < MinMaxAttempts || *maxAttempts > MaxMaxAttempts {
		return 0, validationErrorf("max_attempts", "must be between 1 and 100")
	}
	return *maxAttempts, nil
}

// NormalizeRunAt defaults unset/zero/past values to now; future values are kept.
func NormalizeRunAt(runAt *time.Time, now time.Time) time.Time {
	if runAt == nil || runAt.IsZero() || runAt.Before(now) {
		return now
	}
	return *runAt
}

// ValidateIdempotencyKey accepts empty or printable ASCII up to 255 chars.
func ValidateIdempotencyKey(key string) error {
	if key == "" {
		return nil
	}
	if len(key) > MaxIdempotencyKeyLen {
		return validationErrorf("idempotency_key", "must be at most 255 characters")
	}
	if !isPrintableASCII(key) {
		return validationErrorf("idempotency_key", "must be printable ASCII")
	}
	return nil
}

// NormalizeLeaseDuration defaults 0 to 30s and validates 5s..15m.
func NormalizeLeaseDuration(d time.Duration) (time.Duration, error) {
	if d == 0 {
		return DefaultLeaseDuration, nil
	}
	if d < MinLeaseDuration || d > MaxLeaseDuration {
		return 0, validationErrorf("lease_duration", "must be between 5s and 15m")
	}
	return d, nil
}

// NormalizeMaxJobs defaults 0 to 1 and validates 1..100.
func NormalizeMaxJobs(n int) (int, error) {
	if n == 0 {
		return DefaultMaxJobs, nil
	}
	if n < MinMaxJobs || n > MaxMaxJobs {
		return 0, validationErrorf("max_jobs", "must be between 1 and 100")
	}
	return n, nil
}

// ValidateWorkerID requires 1..128 printable ASCII characters.
func ValidateWorkerID(id string) error {
	if len(id) < 1 || len(id) > MaxWorkerIDLen {
		return validationErrorf("worker_id", "must be between 1 and 128 characters")
	}
	if !isPrintableASCII(id) {
		return validationErrorf("worker_id", "must be printable ASCII")
	}
	return nil
}

// NormalizePageLimit defaults 0 to DefaultPageLimit and validates MinPageLimit..MaxPageLimit.
func NormalizePageLimit(limit int) (int, error) {
	if limit == 0 {
		return DefaultPageLimit, nil
	}
	if limit < MinPageLimit || limit > MaxPageLimit {
		return 0, validationErrorf("limit", "must be between 1 and 200")
	}
	return limit, nil
}

// NormalizeLeaseWait defaults unset values and validates 0..MaxLeaseWait.
func NormalizeLeaseWait(d time.Duration) (time.Duration, error) {
	if d < 0 || d > MaxLeaseWait {
		return 0, validationErrorf("wait", fmt.Sprintf("must be between 0s and %s", MaxLeaseWait))
	}
	return d, nil
}

// NormalizeRetryAllDeadLimit defaults 0 to 1000 and validates 1..10000.
func NormalizeRetryAllDeadLimit(n int) (int, error) {
	if n == 0 {
		return DefaultRetryAllDeadLimit, nil
	}
	if n < MinRetryAllDeadLimit || n > MaxRetryAllDeadLimit {
		return 0, validationErrorf("limit", "must be between 1 and 10000")
	}
	return n, nil
}

// TruncateErrorMessage truncates to MaxErrorMessageBytes without splitting a UTF-8 rune.
func TruncateErrorMessage(msg string) string {
	if len(msg) <= MaxErrorMessageBytes {
		return msg
	}
	truncated := msg[:MaxErrorMessageBytes]
	for !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

func isPrintableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}
