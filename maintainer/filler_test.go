package maintainer_test

import (
	"testing"
	"time"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/petasos/maintainer"
	"github.com/apoydence/petasos/router"
)

func TestFillerOneWideGap(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartFiller(mockRangeMetrics, mockFileSystem,
			maintainer.WithFillerInterval(time.Millisecond),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(0, 9223372036854775807, 2),                    // Valid
			buildRangeName(9223372036854775809, 18446744073709551615, 3), // Valid with gap
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)
		close(mockFileSystem.CreateOutput.Err)

		tb := TB{
			T:                t,
			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
		go serviceMetrics(tb, tb.repeatedFiles, map[string]uint64{
			files[2]: 25,
			files[3]: 25,
		})

		return tb
	})

	o.Spec("it adds a range to fill the gap", func(t TB) {
		files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 1))
		Expect(t, files).To(Contain(
			buildRangeName(9223372036854775808, 9223372036854775808, 4),
		))
	})
}

func TestFillerEndGap(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartFiller(mockRangeMetrics, mockFileSystem,
			maintainer.WithFillerInterval(time.Millisecond),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(0, 9223372036854775807, 2),                    // Valid
			buildRangeName(9223372036854775808, 18446744073709551614, 3), // Valid with gap
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)
		close(mockFileSystem.CreateOutput.Err)

		tb := TB{
			T:                t,
			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
		go serviceMetrics(tb, tb.repeatedFiles, map[string]uint64{
			files[2]: 25,
			files[3]: 25,
		})

		return tb
	})

	o.Spec("it adds a range to fill the gap", func(t TB) {
		files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 1))
		Expect(t, files).To(Contain(
			buildRangeName(18446744073709551615, 18446744073709551615, 4),
		))
	})
}

func TestFillerStartGap(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartFiller(mockRangeMetrics, mockFileSystem,
			maintainer.WithFillerInterval(time.Millisecond),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(10, 9223372036854775807, 2),                   // Valid with gap
			buildRangeName(9223372036854775807, 18446744073709551615, 3), // Valid
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)
		close(mockFileSystem.CreateOutput.Err)

		tb := TB{
			T:                t,
			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
		go serviceMetrics(tb, tb.repeatedFiles, map[string]uint64{
			files[2]: 25,
			files[3]: 25,
		})

		return tb
	})

	o.Spec("it adds a range to fill the gap", func(t TB) {
		files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 1))
		Expect(t, files).To(Contain(
			buildRangeName(0, 9, 4),
		))
	})
}

func TestFillerMidWayGap(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartFiller(mockRangeMetrics, mockFileSystem,
			maintainer.WithFillerInterval(time.Millisecond),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),                   // Stale
			buildRangeName(8000000000000000001, 9223372036854775807, 1), // Stale

			buildRangeName(0, 9223372036854775807, 2),                     // Valid
			buildRangeName(10000000000000000000, 18446744073709551615, 3), // Valid with gap
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)
		close(mockFileSystem.CreateOutput.Err)

		tb := TB{
			T: t,

			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
		go serviceMetrics(tb, tb.repeatedFiles, map[string]uint64{
			files[2]: 25,
			files[3]: 25,
		})

		return tb
	})

	o.Spec("it adds a range to fill the gap", func(t TB) {
		files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 1))
		Expect(t, files).To(Contain(
			buildRangeName(9223372036854775808, 9999999999999999999, 4),
		))
	})
}

func TestFillerGapFromErrs(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TB {
		mockFileSystem := newMockFileSystem()
		mockRangeMetrics := newMockRangeMetrics()
		maintainer.StartFiller(mockRangeMetrics, mockFileSystem,
			maintainer.WithFillerInterval(time.Millisecond),
			maintainer.WithFillerMinCount(0),
		)

		files := []string{
			buildRangeName(0, 8000000000000000000, 0),
			buildRangeName(8000000000000000001, 18446744073709551615, 1),
		}

		testhelpers.AlwaysReturn(mockFileSystem.ListOutput.File, files)
		close(mockFileSystem.ListOutput.Err)
		close(mockFileSystem.CreateOutput.Err)

		tb := TB{
			T: t,

			files:            files,
			repeatedFiles:    make(chan string, 100),
			mockFileSystem:   mockFileSystem,
			mockRangeMetrics: mockRangeMetrics,
		}
		go serviceMetricsWithErrs(tb, tb.repeatedFiles, map[string]router.Metric{
			files[0]: router.Metric{WriteCount: 25},
			files[1]: router.Metric{WriteCount: 25, ErrCount: 10},
		})

		return tb
	})

	o.Spec("it adds a range to fill the gap", func(t TB) {
		files := stripRand(toSlice(t.mockFileSystem.CreateInput.File, 1))
		Expect(t, files).To(Contain(
			buildRangeName(8000000000000000001, 18446744073709551615, 2),
		))
	})
}

func serviceMetricsWithErrs(t TB, repeater chan string, m map[string]router.Metric) {
	for file := range t.mockRangeMetrics.MetricsInput.File {
		t.mockRangeMetrics.MetricsOutput.Metric <- m[file]
		t.mockRangeMetrics.MetricsOutput.Err <- nil
		repeater <- file
	}
}
