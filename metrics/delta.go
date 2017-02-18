package metrics

import "github.com/apoydence/petasos/router"

type Metrics interface {
	Metrics(file string) (metric router.Metric, err error)
}

type Delta struct {
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
	if len(d.data) > d.cacheSize {
		d.data = make(map[string]router.Metric)
	}

	current, err := d.metrics.Metrics(file)
	if err != nil {
		return router.Metric{}, err
	}

	prev, ok := d.data[file]
	if !ok {
		d.data[file] = current
		return router.Metric{}, nil
	}

	return router.Metric{
		WriteCount: current.WriteCount - prev.WriteCount,
		ErrCount:   current.ErrCount - prev.ErrCount,
	}, nil
}
