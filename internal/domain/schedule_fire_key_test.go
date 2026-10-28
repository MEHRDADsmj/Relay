package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFireIdempotencyKeyRoundTrip(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	key := FireIdempotencyKey(id, time.Unix(1700000000, 0).UTC())
	if key != "schedule:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1700000000" {
		t.Fatalf("key = %q", key)
	}
	if FireIdempotencyPrefix(id) != "schedule:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:" {
		t.Fatalf("prefix = %q", FireIdempotencyPrefix(id))
	}
	got, ok := ScheduleIDFromFireKey(key)
	if !ok || got != ScheduleID(id) {
		t.Fatalf("got=%v ok=%v", got, ok)
	}
	if _, ok := ScheduleIDFromFireKey("not-a-schedule-key"); ok {
		t.Fatal("expected false for non-schedule key")
	}
}
