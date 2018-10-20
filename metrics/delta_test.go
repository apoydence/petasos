package metrics_test

import (
	"fmt"
	"testing"

	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/petasos/metrics"
	"github.com/poy/petasos/router"
)

type TD struct {
	*testing.T
	mockMetrics *mockMetrics
	calc        *metrics.Delta
}

func TestRateCalc(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TD {
		mockMetrics := newMockMetrics()
		return TD{
			T:           t,
			calc:        metrics.NewDelta(10, mockMetrics),
			mockMetrics: mockMetrics,
		}
	})

	o.Group("when the parent metrics reader does not return an error", func() {
		o.BeforeEach(func(t TD) TD {
			close(t.mockMetrics.MetricsOutput.Err)
			return t
		})

		o.Spec("it does not return an error", func(t TD) {
			t.mockMetrics.MetricsOutput.Metric <- router.Metric{
				WriteCount: 5,
				ErrCount:   3,
			}

			_, err := t.calc.Metrics("some-file")
			Expect(t, err == nil).To(BeTrue())
		})

		o.Spec("it returns 0 when it does not have prev data", func(t TD) {
			t.mockMetrics.MetricsOutput.Metric <- router.Metric{
				WriteCount: 5,
				ErrCount:   3,
			}

			m, _ := t.calc.Metrics("some-file")
			Expect(t, m.WriteCount).To(Equal(uint64(0)))
			Expect(t, m.ErrCount).To(Equal(uint64(0)))
		})

		o.Spec("it returns the delta from the time before", func(t TD) {
			t.mockMetrics.MetricsOutput.Metric <- router.Metric{
				WriteCount: 5,
				ErrCount:   3,
			}

			m, _ := t.calc.Metrics("some-file")

			t.mockMetrics.MetricsOutput.Metric <- router.Metric{
				WriteCount: 7,
				ErrCount:   5,
			}
			m, _ = t.calc.Metrics("some-file")
			Expect(t, m.WriteCount).To(Equal(uint64(2)))
			Expect(t, m.ErrCount).To(Equal(uint64(2)))
		})

		o.Spec("it uses the correct file", func(t TD) {
			t.mockMetrics.MetricsOutput.Metric <- router.Metric{
				WriteCount: 5,
				ErrCount:   3,
			}

			t.calc.Metrics("some-file")
			Expect(t, t.mockMetrics.MetricsInput.File).To(
				Chain(Receive(), Equal("some-file")),
			)
		})

		o.Spec("it clears after enough files", func(t TD) {
			for i := 0; i < 11; i++ {
				t.mockMetrics.MetricsOutput.Metric <- router.Metric{
					WriteCount: 5,
					ErrCount:   3,
				}
				t.calc.Metrics(fmt.Sprintf("some-file-%d", i))
			}
			t.mockMetrics.MetricsOutput.Metric <- router.Metric{
				WriteCount: 6,
				ErrCount:   4,
			}

			m, _ := t.calc.Metrics("some-file-0")
			Expect(t, m.WriteCount).To(Equal(uint64(0)))
			Expect(t, m.ErrCount).To(Equal(uint64(0)))
		})
	})

	o.Group("when the parent metrics reader returns an error", func() {
		o.BeforeEach(func(t TD) TD {
			t.mockMetrics.MetricsOutput.Err <- fmt.Errorf("some-err")
			close(t.mockMetrics.MetricsOutput.Metric)
			return t
		})

		o.Spec("it returns an error", func(t TD) {
			_, err := t.calc.Metrics("some-file-0")
			Expect(t, err == nil).To(BeFalse())
		})
	})

}
