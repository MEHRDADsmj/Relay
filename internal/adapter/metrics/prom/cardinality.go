package prom

import "sync"

const otherJobType = "__other__"

// seriesCap maps (queue, job_type) to a capped set of label values.
type seriesCap struct {
	mu     sync.Mutex
	max    int
	seen   map[string]struct{}
	capped map[string]struct{} // keys already mapped to __other__
}

func newSeriesCap(max int) *seriesCap {
	if max <= 0 {
		max = 1000
	}
	return &seriesCap{
		max:    max,
		seen:   make(map[string]struct{}),
		capped: make(map[string]struct{}),
	}
}

func pairKey(queue, jobType string) string {
	return queue + "\x00" + jobType
}

// jobType returns jobType or __other__ once the distinct-pair budget is exhausted.
func (c *seriesCap) jobType(queue, jobType string) string {
	key := pairKey(queue, jobType)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.capped[key]; ok {
		return otherJobType
	}
	if _, ok := c.seen[key]; ok {
		return jobType
	}
	if len(c.seen) >= c.max {
		c.capped[key] = struct{}{}
		return otherJobType
	}
	c.seen[key] = struct{}{}
	return jobType
}

// KnownPairs returns how many distinct pairs are currently tracked (for tests).
func (c *seriesCap) KnownPairs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}
