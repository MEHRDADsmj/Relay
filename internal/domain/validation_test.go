package domain

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestValidateQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		queue   string
		wantErr bool
	}{
		{name: "simple", queue: "default"},
		{name: "with dash and underscore", queue: "my-queue_1"},
		{name: "single char", queue: "a"},
		{name: "max length 64", queue: strings.Repeat("a", 64)},
		{name: "starts with digit", queue: "1queue"},
		{name: "empty", queue: "", wantErr: true},
		{name: "starts with dash", queue: "-bad", wantErr: true},
		{name: "uppercase", queue: "Bad", wantErr: true},
		{name: "too long", queue: strings.Repeat("a", 65), wantErr: true},
		{name: "space", queue: "bad queue", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateQueue(tt.queue)
			if tt.wantErr {
				assertValidationField(t, err, "queue")
				return
			}
			if err != nil {
				t.Fatalf("ValidateQueue(%q) = %v", tt.queue, err)
			}
		})
	}
}

func TestValidateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		typ     string
		wantErr bool
	}{
		{name: "simple", typ: "send_email"},
		{name: "dotted", typ: "billing.charge"},
		{name: "colon and dash", typ: "Job:Type-1"},
		{name: "max length 128", typ: "A" + strings.Repeat("a", 127)},
		{name: "empty", typ: "", wantErr: true},
		{name: "starts with dot", typ: ".bad", wantErr: true},
		{name: "too long", typ: "A" + strings.Repeat("a", 128), wantErr: true},
		{name: "space", typ: "bad type", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateType(tt.typ)
			if tt.wantErr {
				assertValidationField(t, err, "type")
				return
			}
			if err != nil {
				t.Fatalf("ValidateType(%q) = %v", tt.typ, err)
			}
		})
	}
}

func TestNormalizePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      []byte
		want    []byte
		wantErr bool
	}{
		{name: "nil defaults", in: nil, want: []byte("{}")},
		{name: "empty defaults", in: []byte{}, want: []byte("{}")},
		{name: "object", in: []byte(`{"a":1}`), want: []byte(`{"a":1}`)},
		{name: "array", in: []byte(`[1,2]`), want: []byte(`[1,2]`)},
		{name: "invalid json", in: []byte(`{`), wantErr: true},
		{name: "too large", in: bytes.Repeat([]byte("a"), MaxPayloadBytes+1), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizePayload(tt.in)
			if tt.wantErr {
				assertValidationField(t, err, "payload")
				return
			}
			if err != nil {
				t.Fatalf("NormalizePayload() err = %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("NormalizePayload() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizePayload_NearMaxSize(t *testing.T) {
	t.Parallel()

	inner := strings.Repeat("a", MaxPayloadBytes-8)
	payload := []byte(`{"x":"` + inner + `"}`)
	if len(payload) > MaxPayloadBytes {
		t.Fatalf("fixture too large: %d", len(payload))
	}
	got, err := NormalizePayload(payload)
	if err != nil {
		t.Fatalf("NormalizePayload(near-max) = %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload mutated unexpectedly")
	}
}

func TestNormalizePriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      *int
		want    int
		wantErr bool
	}{
		{name: "default unset", in: nil, want: DefaultPriority},
		{name: "zero", in: intPtr(0), want: 0},
		{name: "max", in: intPtr(100), want: 100},
		{name: "min", in: intPtr(-100), want: -100},
		{name: "too high", in: intPtr(101), wantErr: true},
		{name: "too low", in: intPtr(-101), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizePriority(tt.in)
			if tt.wantErr {
				assertValidationField(t, err, "priority")
				return
			}
			if err != nil {
				t.Fatalf("NormalizePriority() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("NormalizePriority() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNormalizeMaxAttempts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      *int
		want    int
		wantErr bool
	}{
		{name: "default", in: nil, want: DefaultMaxAttempts},
		{name: "one", in: intPtr(1), want: 1},
		{name: "hundred", in: intPtr(100), want: 100},
		{name: "zero", in: intPtr(0), wantErr: true},
		{name: "too high", in: intPtr(101), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeMaxAttempts(tt.in)
			if tt.wantErr {
				assertValidationField(t, err, "max_attempts")
				return
			}
			if err != nil {
				t.Fatalf("NormalizeMaxAttempts() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeMaxAttempts() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNormalizeRunAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	zero := time.Time{}

	if got := NormalizeRunAt(nil, now); !got.Equal(now) {
		t.Fatalf("nil → now: got %v", got)
	}
	if got := NormalizeRunAt(&zero, now); !got.Equal(now) {
		t.Fatalf("zero → now: got %v", got)
	}
	if got := NormalizeRunAt(&past, now); !got.Equal(now) {
		t.Fatalf("past treated as now: got %v", got)
	}
	if got := NormalizeRunAt(&future, now); !got.Equal(future) {
		t.Fatalf("future kept: got %v", got)
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "empty ok", key: ""},
		{name: "ascii", key: "order-123"},
		{name: "max 255", key: strings.Repeat("a", 255)},
		{name: "too long", key: strings.Repeat("a", 256), wantErr: true},
		{name: "non-printable", key: "bad\x01key", wantErr: true},
		{name: "tab", key: "bad\tkey", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateIdempotencyKey(tt.key)
			if tt.wantErr {
				assertValidationField(t, err, "idempotency_key")
				return
			}
			if err != nil {
				t.Fatalf("ValidateIdempotencyKey() = %v", err)
			}
		})
	}
}

func TestNormalizeTraceContext(t *testing.T) {
	t.Parallel()

	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	got, err := NormalizeTraceContext(nil)
	if err != nil || got != nil {
		t.Fatalf("nil: got %v err %v", got, err)
	}
	got, err = NormalizeTraceContext(map[string]string{})
	if err != nil || got != nil {
		t.Fatalf("empty: got %v err %v", got, err)
	}
	got, err = NormalizeTraceContext(map[string]string{
		TraceContextKeyParent: parent,
		TraceContextKeyState:  "rojo=00f067aa0ba902b7",
	})
	if err != nil {
		t.Fatalf("valid: %v", err)
	}
	if got[TraceContextKeyParent] != parent {
		t.Fatalf("parent = %q", got[TraceContextKeyParent])
	}
	_, err = NormalizeTraceContext(map[string]string{"baggage": "x"})
	assertValidationField(t, err, "trace_context")
	_, err = NormalizeTraceContext(map[string]string{
		TraceContextKeyParent: parent,
		TraceContextKeyState:  "a",
		"extra":               "b",
	})
	assertValidationField(t, err, "trace_context")
	_, err = NormalizeTraceContext(map[string]string{
		TraceContextKeyParent: strings.Repeat("a", MaxTraceContextValueBytes+1),
	})
	assertValidationField(t, err, "trace_context")
}

func TestNormalizeLeaseDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "default zero", in: 0, want: DefaultLeaseDuration},
		{name: "min", in: 5 * time.Second, want: 5 * time.Second},
		{name: "max", in: 15 * time.Minute, want: 15 * time.Minute},
		{name: "typical", in: 30 * time.Second, want: 30 * time.Second},
		{name: "too short", in: 4 * time.Second, wantErr: true},
		{name: "too long", in: 15*time.Minute + time.Second, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeLeaseDuration(tt.in)
			if tt.wantErr {
				assertValidationField(t, err, "lease_duration")
				return
			}
			if err != nil {
				t.Fatalf("NormalizeLeaseDuration() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeLeaseDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeLeaseWait(t *testing.T) {
	// Not parallel: mutates package var MaxLeaseWait.
	prev := MaxLeaseWait
	t.Cleanup(func() { MaxLeaseWait = prev })

	got, err := NormalizeLeaseWait(0)
	if err != nil || got != 0 {
		t.Fatalf("NormalizeLeaseWait(0) = %v, %v", got, err)
	}
	got, err = NormalizeLeaseWait(30 * time.Second)
	if err != nil || got != 30*time.Second {
		t.Fatalf("NormalizeLeaseWait(30s) = %v, %v", got, err)
	}
	if _, err := NormalizeLeaseWait(31 * time.Second); err == nil {
		t.Fatal("expected error for wait > default MaxLeaseWait")
	}

	MaxLeaseWait = 60 * time.Second
	got, err = NormalizeLeaseWait(45 * time.Second)
	if err != nil || got != 45*time.Second {
		t.Fatalf("NormalizeLeaseWait(45s) with MaxLeaseWait=60s = %v, %v", got, err)
	}
	if _, err := NormalizeLeaseWait(61 * time.Second); err == nil {
		t.Fatal("expected error for wait > configured MaxLeaseWait")
	}
	if _, err := NormalizeLeaseWait(-time.Second); err == nil {
		t.Fatal("expected error for negative wait")
	}
}

func TestNormalizeMaxJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      int
		want    int
		wantErr bool
	}{
		{name: "default zero", in: 0, want: DefaultMaxJobs},
		{name: "one", in: 1, want: 1},
		{name: "hundred", in: 100, want: 100},
		{name: "negative", in: -1, wantErr: true},
		{name: "too high", in: 101, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeMaxJobs(tt.in)
			if tt.wantErr {
				assertValidationField(t, err, "max_jobs")
				return
			}
			if err != nil {
				t.Fatalf("NormalizeMaxJobs() = %v", err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeMaxJobs() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestValidateWorkerID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "ok", id: "worker-1"},
		{name: "max", id: strings.Repeat("w", 128)},
		{name: "empty", id: "", wantErr: true},
		{name: "too long", id: strings.Repeat("w", 129), wantErr: true},
		{name: "non-printable", id: "w\x00", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateWorkerID(tt.id)
			if tt.wantErr {
				assertValidationField(t, err, "worker_id")
				return
			}
			if err != nil {
				t.Fatalf("ValidateWorkerID() = %v", err)
			}
		})
	}
}

func TestTruncateErrorMessage(t *testing.T) {
	t.Parallel()

	short := "boom"
	if got := TruncateErrorMessage(short); got != short {
		t.Fatalf("short: got %q", got)
	}

	exact := strings.Repeat("x", MaxErrorMessageBytes)
	if got := TruncateErrorMessage(exact); got != exact {
		t.Fatalf("exact length truncated unexpectedly: %d", len(got))
	}

	long := strings.Repeat("y", MaxErrorMessageBytes+50)
	got := TruncateErrorMessage(long)
	if len(got) != MaxErrorMessageBytes {
		t.Fatalf("len(got)=%d, want %d", len(got), MaxErrorMessageBytes)
	}

	// Multi-byte runes must not leave a broken trailing sequence.
	multi := strings.Repeat("é", MaxErrorMessageBytes) // 2 bytes each → well over limit
	got = TruncateErrorMessage(multi)
	if len(got) > MaxErrorMessageBytes {
		t.Fatalf("utf8 truncate exceeded: %d", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncate produced invalid UTF-8")
	}

	// Force a mid-rune cut so the validity rewind path runs.
	mid := strings.Repeat("a", MaxErrorMessageBytes-1) + "é"
	got = TruncateErrorMessage(mid)
	if len(got) > MaxErrorMessageBytes {
		t.Fatalf("mid-rune truncate exceeded: %d", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("mid-rune truncate produced invalid UTF-8")
	}
	if got != strings.Repeat("a", MaxErrorMessageBytes-1) {
		t.Fatalf("mid-rune truncate = %q", got)
	}
}

func assertValidationField(t *testing.T, err error, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected ValidationError for %s", field)
	}
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("errors.Is(..., ErrValidation)=false; err=%v", err)
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("errors.As ValidationError failed: %v", err)
	}
	if ve.Field != field {
		t.Fatalf("Field=%q, want %q", ve.Field, field)
	}
}

func intPtr(v int) *int { return &v }
