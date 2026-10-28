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
type cursorPayload struct {
	T  int64  `json:"t"`  // unix milliseconds
	ID string `json:"id"` // job UUID
}

// EncodeCursor returns a base64url-encoded JSON cursor for keyset pagination.
func EncodeCursor(t time.Time, id domain.JobID) (string, error) {
	raw, err := json.Marshal(cursorPayload{
		T:  t.UTC().UnixMilli(),
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
	if p.T == 0 && p.ID == "" {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: missing fields", domain.ErrValidation)
	}
	id, err := uuid.Parse(p.ID)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: cursor: invalid id", domain.ErrValidation)
	}
	return time.UnixMilli(p.T).UTC(), id, nil
}
