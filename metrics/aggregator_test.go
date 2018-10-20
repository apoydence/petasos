package metrics_test

import (
	"testing"

	"github.com/poy/eachers/testhelpers"
	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/petasos/metrics"
	"github.com/poy/petasos/router"
)

type TA struct {
	*testing.T
	agg *metrics.Aggregator

	mockRouters []*mockRouter
}

func TestAggregator(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TA {
		var mockRouters []*mockRouter
		var routers []metrics.Router
		for i := 0; i < 3; i++ {
			mockRouter := newMockRouter()
			mockRouters = append(mockRouters, mockRouter)
			routers = append(routers, mockRouter)
		}

		return TA{
			T:           t,
			agg:         metrics.NewAggregator(routers),
			mockRouters: mockRouters,
		}
	})

	o.Spec("it requests data from each router", func(t TA) {
		for _, mr := range t.mockRouters {
			testhelpers.AlwaysReturn(mr.MetricsOutput.Metric, router.Metric{})
		}
		t.agg.Metrics("some-file")

		Expect(t, t.mockRouters[0].MetricsInput.File).To(
			Chain(Receive(), Equal("some-file")),
		)
		Expect(t, t.mockRouters[1].MetricsInput.File).To(
			Chain(Receive(), Equal("some-file")),
		)
		Expect(t, t.mockRouters[2].MetricsInput.File).To(
			Chain(Receive(), Equal("some-file")),
		)
	})

	o.Spec("it sums each metric", func(t TA) {
		for _, mr := range t.mockRouters {
			testhelpers.AlwaysReturn(mr.MetricsOutput.Metric, router.Metric{
				WriteCount: 5,
				ErrCount:   3,
			})
		}
		metric := t.agg.Metrics("some-file")

		Expect(t, metric.WriteCount).To(Equal(uint64(15)))
		Expect(t, metric.ErrCount).To(Equal(uint64(9)))

	})
}
