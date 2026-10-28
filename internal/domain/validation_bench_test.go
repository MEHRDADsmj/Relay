package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkValidateQueue(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ValidateQueue("bench-queue")
	}
}

func BenchmarkValidateType(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ValidateType("send_welcome_email")
	}
}

func BenchmarkNewJob(b *testing.B) {
	now := time.Now().UTC()
	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	payload := []byte(`{"n":1}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewJob(NewJobParams{
			Queue:   "bench",
			Type:    "bench",
			Payload: payload,
		}, id, now)
		if err != nil {
			b.Fatal(err)
		}
	}
}
