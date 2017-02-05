package router_test

import (
	"testing"

	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/petasos/router"
)

type TM struct {
	*testing.T

	counter *router.Counter
}

func TestMetrics(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TM {
		return TM{
			T:       t,
			counter: router.NewCounter(),
		}
	})

	o.Spec("it reports successes", func(t TM) {
		rn := router.RangeName{Term: 1}
		t.counter.IncSuccess(rn)

		metric := t.counter.Metrics(rn)
		Expect(t, metric.WriteCount).To(Equal(uint64(1)))
		Expect(t, metric.ErrCount).To(Equal(uint64(0)))
	})

	o.Spec("it reports failures", func(t TM) {
		rn := router.RangeName{Term: 1}
		t.counter.IncFailure(rn)

		metric := t.counter.Metrics(rn)
		Expect(t, metric.WriteCount).To(Equal(uint64(0)))
		Expect(t, metric.ErrCount).To(Equal(uint64(1)))
	})
}
