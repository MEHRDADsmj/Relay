package main

import (
	"strings"
	"time"
)

// parseRunAt accepts RFC3339 or a relative duration with leading + or - (e.g. +5m).
func parseRunAt(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, err := time.ParseDuration(s)
		if err != nil {
			return time.Time{}, usageErrorf("invalid --run-at duration %q: %v", s, err)
		}
		return now.Add(d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	if err != nil {
		return time.Time{}, usageErrorf("invalid --run-at %q (want RFC3339 or +5m)", s)
	}
	return t, nil
}
