package metrics

import (
	"sync"

	"github.com/apoydence/petasos/router"
)

type Metrics interface {
	Metrics(file string) (metric router.Metric, err error)
}

type Delta struct {
	mu        sync.Mutex
	metrics   Metrics
	cacheSize int
	data      map[string]router.Metric
}

func NewDelta(cacheSize int, metrics Metrics) *Delta {
	return &Delta{
		metrics:   metrics,
		cacheSize: cacheSize,
		data:      make(map[string]router.Metric),
	}
}

func (d *Delta) Metrics(file string) (metric router.Metric, err error) {
	data := d.fetchData()

	current, err := d.metrics.Metrics(file)
	if err != nil {
		return router.Metric{}, err
	}

	prev, ok := d.cas(data, file, current)
	if !ok {
		return router.Metric{}, nil
	}

	return router.Metric{
		WriteCount: current.WriteCount - prev.WriteCount,
		ErrCount:   current.ErrCount - prev.ErrCount,
	}, nil
}

func (d *Delta) fetchData() map[string]router.Metric {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.data) > d.cacheSize {
		d.data = make(map[string]router.Metric)
	}

	return d.data
}

func (d *Delta) cas(data map[string]router.Metric, file string, current router.Metric) (router.Metric, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	prev, ok := data[file]
	if !ok {
		data[file] = current
		return router.Metric{}, false
	}
	return prev, ok
}
