package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	k := NewKeys("")
	assert.Equal(t, "relay", k.Prefix())
	assert.Equal(t, "relay:job:abc", k.Job("abc"))
	assert.Equal(t, "relay:q:default:scheduled", k.Scheduled("default"))
	assert.Equal(t, "relay:q:default:ready", k.Ready("default"))
	assert.Equal(t, "relay:q:default:leased", k.Leased("default"))
	assert.Equal(t, "relay:idem:default:k1", k.Idempotency("default", "k1"))

	custom := NewKeys("myapp")
	assert.Equal(t, "myapp:job:abc", custom.Job("abc"))
}

func TestReadyScore(t *testing.T) {
	// Higher priority → lower score.
	assert.True(t, readyScore(10, 1000) < readyScore(1, 1000))
	// Same priority → earlier run_at wins.
	assert.True(t, readyScore(1, 1000) < readyScore(1, 2000))
}
