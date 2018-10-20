package metrics

import "github.com/poy/petasos/router"

type Router interface {
	Metrics(file string) (metric router.Metric)
}

type Aggregator struct {
	routers []Router
}

func NewAggregator(routers []Router) *Aggregator {
	return &Aggregator{
		routers: routers,
	}
}

func (a *Aggregator) Metrics(file string) (metric router.Metric) {
	for _, r := range a.routers {
		m := r.Metrics(file)
		metric.WriteCount += m.WriteCount
		metric.ErrCount += m.ErrCount
	}
	return metric
}
