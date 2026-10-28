package app

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

// cursorPayload is the JSON shape inside an opaque page cursor.
// T is a pointer so missing t is distinct from an explicit zero.
type cursorPayload struct {
	T  *int64 `json:"t"`  // unix milliseconds (required, must be > 0)
	ID string `json:"id"` // job UUID (required, non-nil)
}

// EncodeCursor returns a base64url-encoded JSON cursor for keyset pagination.
func EncodeCursor(t time.Time, id domain.JobID) (string, error) {
	ms := t.UTC().UnixMilli()
	raw, err := json.Marshal(cursorPayload{
		T:  &ms,
		ID: id.String(),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// nameCursorPayload is used for name-ordered schedule pagination.
type nameCursorPayload struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// EncodeNameCursor returns a base64url JSON cursor for (name, id) keysets.
func EncodeNameCursor(name string, id uuid.UUID) (string, error) {
	raw, err := json.Marshal(nameCursorPayload{Name: name, ID: id.String()})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeNameCursor parses a name-ordered page cursor.
func DecodeNameCursor(cursor string) (name string, id uuid.UUID, err error) {
	if cursor == "" {
		return "", uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "required"}
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid encoding"}
	}
	var p nameCursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid json"}
	}
	if p.Name == "" || p.ID == "" {
		return "", uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "missing fields"}
	}
	parsed, err := uuid.Parse(p.ID)
	if err != nil || parsed == uuid.Nil {
		return "", uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid id"}
	}
	return p.Name, parsed, nil
}

// DecodeCursor parses an opaque cursor into updated_at time and job id.
func DecodeCursor(cursor string) (time.Time, domain.JobID, error) {
	if cursor == "" {
		return time.Time{}, uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "required"}
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid encoding"}
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return time.Time{}, uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid json"}
	}
	if p.T == nil || *p.T == 0 {
		return time.Time{}, uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid t"}
	}
	if p.ID == "" {
		return time.Time{}, uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "missing id"}
	}
	id, err := uuid.Parse(p.ID)
	if err != nil || id == uuid.Nil {
		return time.Time{}, uuid.Nil, &domain.ValidationError{Field: "cursor", Message: "invalid id"}
	}
	return time.UnixMilli(*p.T).UTC(), id, nil
}
