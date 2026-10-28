package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MEHRDADsmj/relay/internal/app"
	"github.com/MEHRDADsmj/relay/internal/domain"
)

var _ app.StatsRepository = (*Repository)(nil)

// Throughput implements app.StatsRepository.
func (r *Repository) Throughput(ctx context.Context, queue string, from, to time.Time) ([]domain.ThroughputPoint, error) {
	fromU := from.UTC().Truncate(time.Minute).Unix()
	toU := to.UTC().Truncate(time.Minute).Unix()
	if toU < fromU {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, 0, (toU-fromU)/60+1)
	minutes := make([]int64, 0, cap(cmds))
	for m := fromU; m <= toU; m += 60 {
		minutes = append(minutes, m)
		cmds = append(cmds, pipe.HGetAll(ctx, r.keys.Stats(queue, m)))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	out := make([]domain.ThroughputPoint, 0)
	for i, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil {
			return nil, err
		}
		if len(fields) == 0 {
			continue
		}
		p := domain.ThroughputPoint{Minute: time.Unix(minutes[i], 0).UTC()}
		p.Enqueued = parseInt64(fields["enqueued"])
		p.Succeeded = parseInt64(fields["succeeded"])
		p.Failed = parseInt64(fields["failed"])
		p.Dead = parseInt64(fields["dead"])
		if p.Enqueued == 0 && p.Succeeded == 0 && p.Failed == 0 && p.Dead == 0 {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// PurgeStats implements app.MaintenanceRepository.
func (r *Repository) PurgeStats(ctx context.Context, before time.Time) (int, error) {
	cutoff := before.UTC().Truncate(time.Minute).Unix()
	prefix := r.keys.Prefix() + ":stats:"
	var (
		cursor uint64
		n      int
	)
	for {
		keys, next, err := r.client.Scan(ctx, cursor, r.keys.StatsScanPattern(), 100).Result()
		if err != nil {
			return n, err
		}
		for _, key := range keys {
			minute, ok := parseStatsMinute(prefix, key)
			if !ok || minute >= cutoff {
				continue
			}
			if err := r.client.Del(ctx, key).Err(); err != nil {
				return n, err
			}
			n++
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return n, nil
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// parseStatsMinute extracts minute_unix from prefix:stats:{queue}:{minute}.
func parseStatsMinute(prefix, key string) (int64, bool) {
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}
	rest := key[len(prefix):]
	i := strings.LastIndexByte(rest, ':')
	if i < 0 {
		return 0, false
	}
	m, err := strconv.ParseInt(rest[i+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return m, true
}
