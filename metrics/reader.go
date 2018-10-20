package metrics

import "github.com/poy/petasos/router"

type NetworkReader interface {
	ReadMetrics(addr, file string) (metric router.Metric, err error)
}

type Reader struct {
	network NetworkReader
	addrs   []string
}

func NewReader(addrs []string, network NetworkReader) *Reader {
	return &Reader{
		addrs:   addrs,
		network: network,
	}
}

func (r *Reader) Metrics(file string) (metric router.Metric, err error) {
	for _, addr := range r.addrs {
		m, err := r.network.ReadMetrics(addr, file)
		if err != nil {
			return router.Metric{}, err
		}

		metric.WriteCount += m.WriteCount
		metric.ErrCount += m.ErrCount
	}
	return metric, nil
}
