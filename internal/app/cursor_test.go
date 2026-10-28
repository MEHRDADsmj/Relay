package app

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func TestEncodeDecodeCursor_RoundTrip(t *testing.T) {
	t.Parallel()

	id := domain.JobID(uuid.MustParse("44444444-4444-4444-4444-444444444444"))
	ts := time.Date(2026, 10, 28, 12, 0, 0, 123000000, time.UTC)

	enc, err := EncodeCursor(ts, id)
	if err != nil {
		t.Fatalf("EncodeCursor() err = %v", err)
	}
	if enc == "" {
		t.Fatal("EncodeCursor() returned empty")
	}

	gotT, gotID, err := DecodeCursor(enc)
	if err != nil {
		t.Fatalf("DecodeCursor() err = %v", err)
	}
	if !gotT.Equal(ts) {
		t.Fatalf("time = %v, want %v", gotT, ts)
	}
	if gotID != id {
		t.Fatalf("id = %v, want %v", gotID, id)
	}
}

func TestDecodeCursor_Invalid(t *testing.T) {
	t.Parallel()

	validID := "44444444-4444-4444-4444-444444444444"
	tests := []struct {
		name   string
		cursor string
	}{
		{name: "empty", cursor: ""},
		{name: "not base64", cursor: "!!!"},
		{name: "not json", cursor: base64.RawURLEncoding.EncodeToString([]byte("abc"))},
		{name: "missing fields", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"x":1}`))},
		{name: "bad uuid", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"not-a-uuid"}`))},
		{name: "missing t", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"id":"` + validID + `"}`))},
		{name: "t zero", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"t":0,"id":"` + validID + `"}`))},
		{name: "missing id", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1}`))},
		{name: "empty id", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":""}`))},
		{name: "nil uuid", cursor: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"00000000-0000-0000-0000-000000000000"}`))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := DecodeCursor(tt.cursor)
			if err == nil {
				t.Fatal("DecodeCursor() err = nil, want error")
			}
		})
	}
}
