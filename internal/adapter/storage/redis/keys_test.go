package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeys(t *testing.T) {
	k := NewKeys("")
	assert.Equal(t, "relay", k.Prefix())
	assert.Equal(t, "relay:job:abc", k.Job("abc"))
	assert.Equal(t, "relay:job:abc:attempts", k.Attempts("abc"))
	assert.Equal(t, "relay:q:default:scheduled", k.Scheduled("default"))
	assert.Equal(t, "relay:q:default:ready", k.Ready("default"))
	assert.Equal(t, "relay:q:default:leased", k.Leased("default"))
	assert.Equal(t, "relay:q:default:counts", k.Counts("default"))
	assert.Equal(t, "relay:q:default:status:dead", k.Status("default", "dead"))
	assert.Equal(t, "relay:idem:default:k1", k.Idempotency("default", "k1"))
	assert.Equal(t, "relay:queues", k.Queues())
	assert.Equal(t, "relay:queue:default", k.Queue("default"))
	assert.Equal(t, "relay:leased:all", k.LeasedAll())
	assert.Equal(t, "relay:finished", k.Finished())
	assert.Equal(t, "relay:notify:default", k.NotifyChannel("default"))
	assert.Equal(t, "relay:notify:*", k.NotifyPattern())
	assert.Equal(t, "default|abc", LeasedAllMember("default", "abc"))

	custom := NewKeys("myapp")
	assert.Equal(t, "myapp:job:abc", custom.Job("abc"))
}

func TestReadyScore(t *testing.T) {
	// Higher priority → lower score.
	assert.True(t, readyScore(10, 1000) < readyScore(1, 1000))
	// Same priority → earlier run_at wins.
	assert.True(t, readyScore(1, 1000) < readyScore(1, 2000))
}
