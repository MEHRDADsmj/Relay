package redis

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

var (
	_ app.ScheduleRepository = (*Repository)(nil)
	_ app.APIKeyRepository   = (*Repository)(nil)
)

func (r *Repository) scheduleKeys(id string) []string {
	return []string{
		r.keys.Schedule(id),
		r.keys.SchedulesByName(),
		r.keys.SchedulesDue(),
	}
}

// CreateSchedule implements app.ScheduleRepository.
func (r *Repository) CreateSchedule(ctx context.Context, s domain.Schedule) (domain.Schedule, error) {
	id := s.ID.String()
	res, err := scheduleCreateScript.Run(ctx, r.client, r.scheduleKeys(id),
		id,
		s.Name,
		s.Cron,
		s.Timezone,
		s.Queue,
		s.JobType,
		string(s.Payload),
		strconv.Itoa(s.Priority),
		strconv.Itoa(s.MaxAttempts),
		boolToRedis(s.Enabled),
		msString(s.NextRunAt),
		optionalMsString(s.LastRunAt),
		optionalJobIDString(s.LastJobID),
		msString(s.CreatedAt),
		msString(s.UpdatedAt),
	).Result()
	if err != nil {
		return domain.Schedule{}, err
	}
	code, err := scriptStatus(res)
	if err != nil {
		return domain.Schedule{}, err
	}
	switch code {
	case "OK":
		return cloneSchedule(s), nil
	case "CONFLICT":
		return domain.Schedule{}, domain.ErrConflict
	default:
		return domain.Schedule{}, fmt.Errorf("redis create schedule: unknown code %q", code)
	}
}

// GetSchedule implements app.ScheduleRepository.
func (r *Repository) GetSchedule(ctx context.Context, id domain.ScheduleID) (domain.Schedule, error) {
	m, err := r.client.HGetAll(ctx, r.keys.Schedule(id.String())).Result()
	if err != nil {
		return domain.Schedule{}, err
	}
	if len(m) == 0 {
		return domain.Schedule{}, domain.ErrNotFound
	}
	return mapToSchedule(m)
}

// GetScheduleByName implements app.ScheduleRepository.
func (r *Repository) GetScheduleByName(ctx context.Context, name string) (domain.Schedule, error) {
	id, err := r.client.HGet(ctx, r.keys.SchedulesByName(), name).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return domain.Schedule{}, domain.ErrNotFound
		}
		return domain.Schedule{}, err
	}
	uid, err := parseUUID(id)
	if err != nil {
		return domain.Schedule{}, err
	}
	return r.GetSchedule(ctx, uid)
}

// ListSchedules implements app.ScheduleRepository (ordered by name ASC).
func (r *Repository) ListSchedules(ctx context.Context, page domain.PageRequest) (domain.Page[domain.Schedule], error) {
	limit := page.Limit
	if limit <= 0 {
		limit = domain.DefaultPageLimit
	}

	byName, err := r.client.HGetAll(ctx, r.keys.SchedulesByName()).Result()
	if err != nil {
		return domain.Page[domain.Schedule]{}, err
	}

	type entry struct {
		name string
		id   string
	}
	entries := make([]entry, 0, len(byName))
	for name, id := range byName {
		entries = append(entries, entry{name: name, id: id})
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].name != entries[j].name {
			return entries[i].name < entries[j].name
		}
		return entries[i].id < entries[j].id
	})

	var cursorName string
	var cursorID string
	hasCursor := page.Cursor != ""
	if hasCursor {
		name, id, err := app.DecodeNameCursor(page.Cursor)
		if err != nil {
			return domain.Page[domain.Schedule]{}, err
		}
		cursorName, cursorID = name, id.String()
	}

	matched := make([]domain.Schedule, 0, len(entries))
	for _, e := range entries {
		if hasCursor {
			if e.name < cursorName {
				continue
			}
			if e.name == cursorName && e.id <= cursorID {
				continue
			}
		}
		s, err := r.GetSchedule(ctx, mustParseUUID(e.id))
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return domain.Page[domain.Schedule]{}, err
		}
		matched = append(matched, s)
	}

	nextCursor := ""
	if len(matched) > limit {
		last := matched[limit-1]
		enc, err := app.EncodeNameCursor(last.Name, last.ID)
		if err != nil {
			return domain.Page[domain.Schedule]{}, err
		}
		nextCursor = enc
		matched = matched[:limit]
	}
	return domain.Page[domain.Schedule]{Items: matched, NextCursor: nextCursor}, nil
}

// UpdateSchedule implements app.ScheduleRepository with optimistic concurrency.
func (r *Repository) UpdateSchedule(ctx context.Context, s domain.Schedule, expectedUpdatedAt time.Time) (domain.Schedule, error) {
	id := s.ID.String()
	res, err := scheduleUpdateScript.Run(ctx, r.client, r.scheduleKeys(id),
		id,
		s.Name,
		s.Cron,
		s.Timezone,
		s.Queue,
		s.JobType,
		string(s.Payload),
		strconv.Itoa(s.Priority),
		strconv.Itoa(s.MaxAttempts),
		boolToRedis(s.Enabled),
		msString(s.NextRunAt),
		optionalMsString(s.LastRunAt),
		optionalJobIDString(s.LastJobID),
		msString(s.UpdatedAt),
		msString(expectedUpdatedAt),
	).Result()
	if err != nil {
		return domain.Schedule{}, err
	}
	code, err := scriptStatus(res)
	if err != nil {
		return domain.Schedule{}, err
	}
	switch code {
	case "OK":
		return cloneSchedule(s), nil
	case "NOT_FOUND":
		return domain.Schedule{}, domain.ErrNotFound
	case "CONFLICT":
		return domain.Schedule{}, domain.ErrConflict
	default:
		return domain.Schedule{}, fmt.Errorf("redis update schedule: unknown code %q", code)
	}
}

// DeleteSchedule implements app.ScheduleRepository.
func (r *Repository) DeleteSchedule(ctx context.Context, id domain.ScheduleID) error {
	res, err := scheduleDeleteScript.Run(ctx, r.client, r.scheduleKeys(id.String()), id.String()).Result()
	if err != nil {
		return err
	}
	code, err := scriptStatus(res)
	if err != nil {
		return err
	}
	switch code {
	case "OK":
		return nil
	case "NOT_FOUND":
		return domain.ErrNotFound
	default:
		return fmt.Errorf("redis delete schedule: unknown code %q", code)
	}
}

// DueSchedules implements app.ScheduleRepository.
func (r *Repository) DueSchedules(ctx context.Context, now time.Time, limit int) ([]domain.Schedule, error) {
	if limit <= 0 {
		limit = 100
	}
	ids, err := r.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:     r.keys.SchedulesDue(),
		Start:   "-inf",
		Stop:    strconv.FormatInt(toUnixMs(now), 10),
		ByScore: true,
		Offset:  0,
		Count:   int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.Schedule, 0, len(ids))
	for _, id := range ids {
		s, err := r.GetSchedule(ctx, mustParseUUID(id))
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// MarkFired implements app.ScheduleRepository.
func (r *Repository) MarkFired(ctx context.Context, id domain.ScheduleID, expectedNext, firedAt, next time.Time, jobID domain.JobID) error {
	res, err := scheduleMarkFiredScript.Run(ctx, r.client,
		[]string{r.keys.Schedule(id.String()), r.keys.SchedulesDue()},
		id.String(),
		msString(expectedNext),
		msString(firedAt),
		msString(next),
		jobID.String(),
	).Result()
	if err != nil {
		return err
	}
	code, err := scriptStatus(res)
	if err != nil {
		return err
	}
	switch code {
	case "OK":
		return nil
	case "NOT_FOUND":
		return domain.ErrNotFound
	case "CONFLICT":
		return domain.ErrConflict
	default:
		return fmt.Errorf("redis mark fired: unknown code %q", code)
	}
}

// CreateAPIKey implements app.APIKeyRepository.
func (r *Repository) CreateAPIKey(ctx context.Context, key domain.APIKey) (domain.APIKey, error) {
	res, err := apiKeyCreateScript.Run(ctx, r.client,
		[]string{r.keys.APIKey(key.ID), r.keys.APIKeysByPrefix()},
		key.ID,
		key.Name,
		key.Prefix,
		hex.EncodeToString(key.Hash),
		joinScopes(key.Scopes),
		msString(key.CreatedAt),
		optionalMsString(key.LastUsedAt),
		optionalMsString(key.ExpiresAt),
		optionalMsString(key.RevokedAt),
	).Result()
	if err != nil {
		return domain.APIKey{}, err
	}
	code, err := scriptStatus(res)
	if err != nil {
		return domain.APIKey{}, err
	}
	switch code {
	case "OK":
		return cloneAPIKey(key), nil
	case "CONFLICT":
		return domain.APIKey{}, domain.ErrConflict
	default:
		return domain.APIKey{}, fmt.Errorf("redis create api key: unknown code %q", code)
	}
}

// GetAPIKey implements app.APIKeyRepository.
func (r *Repository) GetAPIKey(ctx context.Context, id string) (domain.APIKey, error) {
	m, err := r.client.HGetAll(ctx, r.keys.APIKey(id)).Result()
	if err != nil {
		return domain.APIKey{}, err
	}
	if len(m) == 0 {
		return domain.APIKey{}, domain.ErrNotFound
	}
	return mapToAPIKey(m)
}

// GetAPIKeyByPrefix implements app.APIKeyRepository.
func (r *Repository) GetAPIKeyByPrefix(ctx context.Context, prefix string) (domain.APIKey, error) {
	id, err := r.client.HGet(ctx, r.keys.APIKeysByPrefix(), prefix).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return domain.APIKey{}, domain.ErrNotFound
		}
		return domain.APIKey{}, err
	}
	return r.GetAPIKey(ctx, id)
}

// ListAPIKeys implements app.APIKeyRepository.
func (r *Repository) ListAPIKeys(ctx context.Context) ([]domain.APIKey, error) {
	ids, err := r.client.HVals(ctx, r.keys.APIKeysByPrefix()).Result()
	if err != nil {
		return nil, err
	}
	out := make([]domain.APIKey, 0, len(ids))
	for _, id := range ids {
		k, err := r.GetAPIKey(ctx, id)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, k)
	}
	return out, nil
}

// RevokeAPIKey implements app.APIKeyRepository.
func (r *Repository) RevokeAPIKey(ctx context.Context, id string, revokedAt time.Time) (domain.APIKey, error) {
	key := r.keys.APIKey(id)
	n, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return domain.APIKey{}, err
	}
	if n == 0 {
		return domain.APIKey{}, domain.ErrNotFound
	}
	if err := r.client.HSet(ctx, key, "revoked_at", msString(revokedAt)).Err(); err != nil {
		return domain.APIKey{}, err
	}
	return r.GetAPIKey(ctx, id)
}

// UpdateLastUsed implements app.APIKeyRepository.
func (r *Repository) UpdateLastUsed(ctx context.Context, id string, at time.Time) error {
	key := r.keys.APIKey(id)
	n, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return domain.ErrNotFound
	}
	return r.client.HSet(ctx, key, "last_used_at", msString(at)).Err()
}

func scriptStatus(res any) (string, error) {
	arr, err := interfaceSliceToStrings(res)
	if err != nil || len(arr) < 1 {
		return "", fmt.Errorf("redis script: bad status result %v: %w", res, err)
	}
	return arr[0], nil
}

func joinScopes(scopes []domain.Scope) string {
	if len(scopes) == 0 {
		return ""
	}
	parts := make([]string, len(scopes))
	for i, s := range scopes {
		parts[i] = string(s)
	}
	return strings.Join(parts, ",")
}
