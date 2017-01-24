//go:generate hel

package metrics_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/petasos/metrics"
	"github.com/apoydence/petasos/router"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

type TR struct {
	*testing.T
	reader            *metrics.Reader
	mockNetworkReader *mockNetworkReader

	addrs []string
}

func TestMetrics(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		addrs := []string{"a", "b", "c"}
		mockNetworkReader := newMockNetworkReader()

		return TR{
			T:                 t,
			addrs:             addrs,
			reader:            metrics.NewReader(addrs, mockNetworkReader),
			mockNetworkReader: mockNetworkReader,
		}
	})

	o.Group("when network does not return an error", func() {
		o.BeforeEach(func(t TR) TR {
			close(t.mockNetworkReader.ReadMetricsOutput.Err)
			return t
		})

		o.Spec("it requests metrics from each router", func(t TR) {
			testhelpers.AlwaysReturn(t.mockNetworkReader.ReadMetricsOutput.Metric, router.Metric{})
			t.reader.Metrics("some-file")

			s := toSlice(t.mockNetworkReader.ReadMetricsInput.Addr, 3)
			Expect(t, s).To(Contain(t.addrs[0], t.addrs[1], t.addrs[2]))
			Expect(t, t.mockNetworkReader.ReadMetricsInput.File).To(
				Chain(Receive(), Equal("some-file")),
			)
		})

		o.Spec("aggregates all the results", func(t TR) {
			testhelpers.AlwaysReturn(t.mockNetworkReader.ReadMetricsOutput.Metric, router.Metric{
				WriteCount: 1,
				ErrCount:   1,
			})

			metric, err := t.reader.Metrics("some-file")
			Expect(t, err == nil).To(BeTrue())

			Expect(t, metric.WriteCount).To(Equal(uint64(3)))
			Expect(t, metric.ErrCount).To(Equal(uint64(3)))
		})
	})

	o.Group("when the network returns an error", func() {
		o.BeforeEach(func(t TR) TR {
			testhelpers.AlwaysReturn(t.mockNetworkReader.ReadMetricsOutput.Err, fmt.Errorf("some-error"))
			return t
		})

		o.Spec("it returns an error", func(t TR) {
			testhelpers.AlwaysReturn(t.mockNetworkReader.ReadMetricsOutput.Metric, router.Metric{})
			_, err := t.reader.Metrics("some-file")

			Expect(t, err == nil).To(BeFalse())
		})
	})
}

func toSlice(c <-chan string, count int) []string {
	var results []string
	for i := 0; i < count; i++ {
		select {
		case x := <-c:
			results = append(results, x)
		case <-time.NewTimer(time.Second).C:
			panic(fmt.Sprintf("expected to receive (i=%d)", i))
		}
	}
	return results
}
