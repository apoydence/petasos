//go:generate hel

package maintainer_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/poy/eachers/testhelpers"
	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/petasos/maintainer"
	"github.com/poy/petasos/router"
)

type TB struct {
	*testing.T

	files            []string
	repeatedFiles    chan string
	mockFileSystem   *mockFileSystem
	mockRangeMetrics *mockRangeMetrics
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}

	os.Exit(m.Run())
}

func TestBalancer(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartBalancer(mockRangeMetrics, mockFileSystem,
			maintainer.WithBalancerInterval(time.Millisecond),
			maintainer.WithMinCount(1),
			maintainer.WithMaxCount(10),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(0, 9223372036854775807, 2),                    // Valid
			buildRangeName(9223372036854775808, 18446744073709551615, 3), // Valid
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)

		return TB{
			T: t,

			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
	})

	o.Group("when one range has too much data", func() {
		o.BeforeEach(func(t TB) TB {
			close(t.mockFileSystem.CreateOutput.Err)

			go serviceMetrics(t, t.repeatedFiles, map[string]uint64{
				t.files[2]: 2600,
				t.files[3]: 25,
			})

			return t
		})

		o.Spec("it queries each file", func(t TB) {
			s := stripRand(toSlice(t.repeatedFiles, 4))

			Expect(t, s).To(Contain(t.files[2], t.files[3]))
		})

		o.Spec("it splits the range", func(t TB) {
			files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 2))
			Expect(t, files).To(Contain(
				buildRangeName(0, 4611686018427387903, 4),
				buildRangeName(4611686018427387904, 9223372036854775807, 5),
			))
		})

		o.Spec("it populates the rand field", func(t TB) {
			files := toSlice(t.mockFileSystem.CreateInput.File, 2)

			var rn router.RangeName
			err := json.Unmarshal([]byte(files[0]), &rn)
			Expect(t, err == nil).To(BeTrue())
			Expect(t, rn.Rand).To(Not(Equal(int64(0))))
		})
	})

	o.Group("when one range has too little data", func() {
		o.BeforeEach(func(t TB) TB {
			close(t.mockFileSystem.CreateOutput.Err)

			go serviceMetrics(t, t.repeatedFiles, map[string]uint64{
				t.files[2]: 25,
				t.files[3]: 1,
			})

			return t
		})

		o.Spec("it combines the range", func(t TB) {
			files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 1))
			Expect(t, files).To(Contain(
				buildRangeName(0, 18446744073709551615, 4),
			))
		})
	})
}

func TestBalancerMaxCounts(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartBalancer(mockRangeMetrics, mockFileSystem,
			maintainer.WithBalancerInterval(time.Millisecond),
			maintainer.WithMaxCount(2),
			maintainer.WithMinCount(1),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(0, 9223372036854775807, 2),                    // Valid
			buildRangeName(9223372036854775808, 18446744073709551615, 3), // Valid
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)

		return TB{
			T: t,

			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
	})

	o.Group("when one range has too much data but there are too many ranges", func() {
		o.BeforeEach(func(t TB) TB {
			close(t.mockFileSystem.CreateOutput.Err)

			go serviceMetrics(t, t.repeatedFiles, map[string]uint64{
				t.files[2]: 2600,
				t.files[3]: 25,
			})

			return t
		})

		o.Spec("it does not split the range", func(t TB) {
			Expect(t, t.mockFileSystem.CreateCalled).To(Always(HaveLen(0)))
		})
	})
}

func TestBalancerMinCounts(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartBalancer(mockRangeMetrics, mockFileSystem,
			maintainer.WithBalancerInterval(time.Millisecond),
			maintainer.WithMinCount(2),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(0, 9223372036854775807, 2),                    // Valid
			buildRangeName(9223372036854775808, 18446744073709551615, 3), // Valid
		}

		return TB{
			T: t,

			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
	})

	o.Group("when one range has too little data but there are too few ranges", func() {
		o.BeforeEach(func(t TB) TB {
			close(t.mockFileSystem.CreateOutput.Err)

			testhelpers.AlwaysReturn(t.mockFileSystem.ListOutput.File, t.files)
			close(t.mockFileSystem.ListOutput.Err)

			go serviceMetrics(t, t.repeatedFiles, map[string]uint64{
				t.files[2]: 25,
				t.files[3]: 1,
			})

			return t
		})

		o.Spec("it does not combine the range", func(t TB) {
			Expect(t, t.mockFileSystem.CreateCalled).To(Always(HaveLen(0)))
		})
	})

}

func TestBalancerEmptyRanges(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartBalancer(mockRangeMetrics, mockFileSystem,
			maintainer.WithBalancerInterval(time.Millisecond),
			maintainer.WithMinCount(3),
		)

		return TB{
			T: t,

			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
	})

	o.Group("when there are no ranges", func() {
		o.BeforeEach(func(t TB) TB {
			close(t.mockFileSystem.CreateOutput.Err)

			testhelpers.AlwaysReturn(t.mockFileSystem.ListOutput.File, []string{})
			close(t.mockFileSystem.ListOutput.Err)

			go serviceMetrics(t, t.repeatedFiles, map[string]uint64{})

			return t
		})

		o.Spec("it adds the minimum routes", func(t TB) {
			s := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 3))
			Expect(t, s).To(Contain(
				buildRangeName(0, 6148914691236517205, 0),
				buildRangeName(6148914691236517206, 12297829382473034410, 1),
				buildRangeName(12297829382473034411, 18446744073709551615, 2),
			))
		})
	})

	o.Group("when listing does not work", func() {
		o.BeforeEach(func(t TB) TB {
			close(t.mockFileSystem.CreateOutput.Err)

			close(t.mockFileSystem.ListOutput.File)
			testhelpers.AlwaysReturn(t.mockFileSystem.ListOutput.Err, fmt.Errorf("some-error"))
			return t
		})

		o.Spec("does not seed routes", func(t TB) {
			Expect(t, t.mockFileSystem.CreateCalled).To(Always(HaveLen(0)))
		})
	})
}

func serviceMetrics(t TB, repeater chan string, m map[string]uint64) {
	for file := range t.mockRangeMetrics.MetricsInput.File {
		t.mockRangeMetrics.MetricsOutput.Metric <- router.Metric{WriteCount: m[file]}
		t.mockRangeMetrics.MetricsOutput.Err <- nil
		repeater <- file
	}
}

func stripRand(s []string) (results []string) {
	for _, ss := range s {
		var rn router.RangeName
		if err := json.Unmarshal([]byte(ss), &rn); err != nil {
			panic(err)
		}
		rn.Rand = 0

		data, err := json.Marshal(rn)
		if err != nil {
			panic(err)
		}
		results = append(results, string(data))
	}
	return results
}

func toSlice(c chan string, count int) []string {
	var result []string
	for i := 0; i < count; i++ {
		select {
		case x := <-c:
			result = append(result, x)
		case <-time.NewTimer(time.Second).C:
			panic("not enough elements")
		}
	}
	return result
}

func buildRangeName(low, high, term uint64) string {
	rn := router.RangeName{
		Low:  low,
		High: high,
		Term: term,
	}

	j, _ := json.Marshal(rn)
	return string(j)
}
