package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMaintenance_ZeroConfigAppliesRetentionDefault(t *testing.T) {
	t.Parallel()
	m := NewMaintenance(nil, SystemClock{}, MaintenanceConfig{}, nil)
	assert.Equal(t, 5*time.Second, m.cfg.ReapInterval)
	assert.Equal(t, 500, m.cfg.ReapBatch)
	assert.Equal(t, time.Minute, m.cfg.PurgeInterval)
	assert.Equal(t, 1000, m.cfg.PurgeBatch)
	assert.Equal(t, 168*time.Hour, m.cfg.Retention)
}

func TestNewMaintenance_ExplicitRetentionZeroMeansNeverPurge(t *testing.T) {
	t.Parallel()
	m := NewMaintenance(nil, SystemClock{}, MaintenanceConfig{
		ReapInterval: 5 * time.Second,
		Retention:    0,
	}, nil)
	assert.Equal(t, 5*time.Second, m.cfg.ReapInterval)
	assert.Equal(t, time.Duration(0), m.cfg.Retention)
}

func TestNewMaintenance_ExplicitRetentionPreserved(t *testing.T) {
	t.Parallel()
	m := NewMaintenance(nil, SystemClock{}, MaintenanceConfig{
		Retention: 24 * time.Hour, // ReapInterval 0 → full defaults, but keep set Retention
	}, nil)
	assert.Equal(t, 5*time.Second, m.cfg.ReapInterval)
	assert.Equal(t, 24*time.Hour, m.cfg.Retention)
}
