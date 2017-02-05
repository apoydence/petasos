package router

import "sync"

type Counter struct {
	mu      sync.Mutex
	metrics map[RangeName]Metric
}

type Metric struct {
	WriteCount, ErrCount uint64
}

func NewCounter() *Counter {
	return &Counter{
		metrics: make(map[RangeName]Metric),
	}
}

func (c *Counter) IncSuccess(rn RangeName) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.metrics[rn]
	m.WriteCount++

	c.metrics[rn] = m
}

func (c *Counter) IncFailure(rn RangeName) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.metrics[rn]
	m.ErrCount++

	c.metrics[rn] = m
}

func (c *Counter) Metrics(rn RangeName) (metric Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.metrics[rn]
}
