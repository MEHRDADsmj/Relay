package redis

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/MEHRDADsmj/relay/internal/domain"
)

func optionalJobIDString(id *domain.JobID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func cloneSchedule(s domain.Schedule) domain.Schedule {
	out := s
	if s.Payload != nil {
		out.Payload = append([]byte(nil), s.Payload...)
	}
	if s.LastRunAt != nil {
		t := *s.LastRunAt
		out.LastRunAt = &t
	}
	if s.LastJobID != nil {
		id := *s.LastJobID
		out.LastJobID = &id
	}
	return out
}

func cloneAPIKey(k domain.APIKey) domain.APIKey {
	out := k
	if k.Hash != nil {
		out.Hash = append([]byte(nil), k.Hash...)
	}
	if k.Scopes != nil {
		out.Scopes = append([]domain.Scope(nil), k.Scopes...)
	}
	if k.LastUsedAt != nil {
		t := *k.LastUsedAt
		out.LastUsedAt = &t
	}
	if k.ExpiresAt != nil {
		t := *k.ExpiresAt
		out.ExpiresAt = &t
	}
	if k.RevokedAt != nil {
		t := *k.RevokedAt
		out.RevokedAt = &t
	}
	return out
}

func mapToSchedule(m map[string]string) (domain.Schedule, error) {
	id, err := uuid.Parse(m["id"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule id: %w", err)
	}
	priority, err := strconv.Atoi(m["priority"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule priority: %w", err)
	}
	maxAttempts, err := strconv.Atoi(m["max_attempts"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule max_attempts: %w", err)
	}
	nextRunAt, err := parseMs(m["next_run_at"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule next_run_at: %w", err)
	}
	createdAt, err := parseMs(m["created_at"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule created_at: %w", err)
	}
	updatedAt, err := parseMs(m["updated_at"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule updated_at: %w", err)
	}
	lastRunAt, err := parseOptionalMs(m["last_run_at"])
	if err != nil {
		return domain.Schedule{}, fmt.Errorf("redis schedule last_run_at: %w", err)
	}
	var lastJobID *domain.JobID
	if m["last_job_id"] != "" {
		jid, err := uuid.Parse(m["last_job_id"])
		if err != nil {
			return domain.Schedule{}, fmt.Errorf("redis schedule last_job_id: %w", err)
		}
		lastJobID = &jid
	}
	payload := []byte(m["payload"])
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	return domain.Schedule{
		ID:          id,
		Name:        m["name"],
		Cron:        m["cron"],
		Timezone:    m["timezone"],
		Queue:       m["queue"],
		JobType:     m["job_type"],
		Payload:     payload,
		Priority:    priority,
		MaxAttempts: maxAttempts,
		Enabled:     boolFromRedis(m["enabled"]),
		NextRunAt:   nextRunAt,
		LastRunAt:   lastRunAt,
		LastJobID:   lastJobID,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

func mapToAPIKey(m map[string]string) (domain.APIKey, error) {
	hash, err := hex.DecodeString(m["hash"])
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("redis api key hash: %w", err)
	}
	createdAt, err := parseMs(m["created_at"])
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("redis api key created_at: %w", err)
	}
	lastUsedAt, err := parseOptionalMs(m["last_used_at"])
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("redis api key last_used_at: %w", err)
	}
	expiresAt, err := parseOptionalMs(m["expires_at"])
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("redis api key expires_at: %w", err)
	}
	revokedAt, err := parseOptionalMs(m["revoked_at"])
	if err != nil {
		return domain.APIKey{}, fmt.Errorf("redis api key revoked_at: %w", err)
	}
	var scopes []domain.Scope
	if m["scopes"] != "" {
		parts := strings.Split(m["scopes"], ",")
		scopes = make([]domain.Scope, len(parts))
		for i, p := range parts {
			scopes[i] = domain.Scope(p)
		}
	}
	return domain.APIKey{
		ID:         m["id"],
		Name:       m["name"],
		Prefix:     m["prefix"],
		Hash:       hash,
		Scopes:     scopes,
		CreatedAt:  createdAt,
		LastUsedAt: lastUsedAt,
		ExpiresAt:  expiresAt,
		RevokedAt:  revokedAt,
	}, nil
}

func parseUUID(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("redis uuid: %w", err)
	}
	return id, nil
}

func mustParseUUID(s string) uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return id
}
