package maintainer

import "time"

func WithFillerInterval(interval time.Duration) func(c *fillerConfig) {
	return func(c *fillerConfig) {
		c.interval = interval
	}
}

func WithFillerMinCount(count uint64) func(c *fillerConfig) {
	return func(c *fillerConfig) {
		c.min = count
	}
}

func WithBalancerInterval(interval time.Duration) func(c *balancerConfig) {
	return func(c *balancerConfig) {
		c.interval = interval
	}
}

func WithMaxWritesPerInterval(writes uint64) func(c *balancerConfig) {
	return func(c *balancerConfig) {
		c.maxPerInterval = writes
	}
}

func WithMinWritesPerInterval(writes uint64) func(c *balancerConfig) {
	return func(c *balancerConfig) {
		c.minPerInterval = writes
	}
}

func WithMinCount(count uint64) func(c *balancerConfig) {
	return func(c *balancerConfig) {
		c.min = count
	}
}

func WithMaxCount(count uint64) func(c *balancerConfig) {
	return func(c *balancerConfig) {
		c.max = count
	}
}
