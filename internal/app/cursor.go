package app

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// DecodeCursor parses an opaque cursor into updated_at time and job id.
func DecodeCursor(cursor string) (time.Time, domain.JobID, error) {
	if cursor == "" {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: required", domain.ErrValidation)
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: invalid encoding", domain.ErrValidation)
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: invalid json", domain.ErrValidation)
	}
	if p.T == nil || *p.T == 0 {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: invalid t", domain.ErrValidation)
	}
	if p.ID == "" {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: missing id", domain.ErrValidation)
	}
	id, err := uuid.Parse(p.ID)
	if err != nil || id == uuid.Nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: invalid id", domain.ErrValidation)
	}
	return time.UnixMilli(*p.T).UTC(), id, nil
}
