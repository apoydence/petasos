//go:generate hel

package router_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/poy/onpar"
	. "github.com/poy/onpar/expect"
	. "github.com/poy/onpar/matchers"
	"github.com/poy/petasos/router"
)

type TR struct {
	*testing.T

	mockFileSystem     *mockFileSystem
	mockHasher         *mockHasher
	mockWriter         *mockWriter
	mockMetricsCounter *mockMetricsCounter

	r *router.Router
}

func TestRouter(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		mockFileSystem := newMockFileSystem()
		mockHasher := newMockHasher()
		mockWriter := newMockWriter()
		mockMetricsCounter := newMockMetricsCounter()

		return TR{
			T:                  t,
			mockFileSystem:     mockFileSystem,
			mockHasher:         mockHasher,
			mockWriter:         mockWriter,
			mockMetricsCounter: mockMetricsCounter,
			r:                  router.New(mockFileSystem, mockHasher, mockMetricsCounter),
		}
	})

	o.Group("when there are two ranges", func() {
		o.BeforeEach(func(t TR) TR {
			t.mockFileSystem.ListOutput.File <- []string{
				buildRangeName(0, 9223372036854775807, 0),
				buildRangeName(9223372036854775808, 18446744073709551615, 3),

				buildRangeName(9223372036854775808, 10000000000000000000, 1),  // Should not use
				buildRangeName(10000000000000000000, 18446744073709551615, 2), // Should not use
			}
			t.mockFileSystem.ListOutput.Err <- nil

			t.mockHasher.HashOutput.Err <- nil

			t.mockFileSystem.WriterOutput.Writer <- t.mockWriter
			t.mockFileSystem.WriterOutput.Err <- nil

			t.mockWriter.WriteOutput.Err <- nil

			return t
		})

		o.Spec("it writes to the file within the upper range", func(t TR) {
			t.mockHasher.HashOutput.Hash <- 10000000000000000000
			err := t.r.Write([]byte("some-data"))
			Expect(t, err == nil).To(BeTrue())

			Expect(t, t.mockFileSystem.WriterInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":9223372036854775808,"High":18446744073709551615,"Term":3,"Rand":0}`)),
			)

			Expect(t, t.mockWriter.WriteInput.Data).To(
				Chain(Receive(), Equal([]byte("some-data"))),
			)
		})

		o.Spec("it writes to the file within the lower range", func(t TR) {
			t.mockHasher.HashOutput.Hash <- 1000000
			err := t.r.Write([]byte("some-data"))
			Expect(t, err == nil).To(BeTrue())

			Expect(t, t.mockFileSystem.WriterInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":0, "High":9223372036854775807,"Term":0,"Rand":0}`)),
			)

			Expect(t, t.mockWriter.WriteInput.Data).To(
				Chain(Receive(), Equal([]byte("some-data"))),
			)
		})

		o.Spec("it reports how many times each router has been written to", func(t TR) {
			t.mockHasher.HashOutput.Hash <- 1000000
			t.r.Write([]byte("some-data"))

			Expect(t, t.mockMetricsCounter.IncSuccessInput.Name).To(ViaPolling(
				Chain(Receive(), Equal(router.RangeName{
					Low:  0,
					High: 9223372036854775807,
					Term: 0,
				})),
			))
		})

		o.Group("when a range becomes invalid", func() {
			o.BeforeEach(func(t TR) TR {
				t.mockHasher.HashOutput.Hash <- 1000000
				t.mockHasher.HashOutput.Hash <- 1000000
				t.mockHasher.HashOutput.Hash <- 1000000
				t.mockHasher.HashOutput.Err <- nil
				t.mockHasher.HashOutput.Err <- nil

				t.mockFileSystem.ListOutput.File <- []string{
					buildRangeName(0, 9223372036854775807, 0),
					buildRangeName(9223372036854775808, 18446744073709551615, 1),
				}
				t.mockFileSystem.ListOutput.Err <- nil

				t.mockFileSystem.WriterOutput.Writer <- t.mockWriter
				t.mockFileSystem.WriterOutput.Err <- nil

				t.mockWriter.WriteOutput.Err <- fmt.Errorf("some-error")
				t.mockWriter.WriteOutput.Err <- nil
				return t
			})

			o.Spec("it requests an updated list", func(t TR) {
				t.r.Write([]byte("some-data"))
				t.r.Write([]byte("some-data"))
				t.r.Write([]byte("some-data"))

				Expect(t, t.mockFileSystem.ListCalled).To(ViaPolling(HaveLen(2)))
			})

			o.Spec("it closes the previous writers", func(t TR) {
				t.r.Write([]byte("some-data"))
				t.r.Write([]byte("some-data"))
				t.r.Write([]byte("some-data"))

				Expect(t, t.mockWriter.CloseCalled).To(ViaPolling(HaveLen(1)))
			})

			o.Spec("it reports how many errors", func(t TR) {
				t.r.Write([]byte("some-data"))
				t.r.Write([]byte("some-data"))
				t.r.Write([]byte("some-data"))

				Expect(t, t.mockMetricsCounter.IncFailureInput.Name).To(ViaPolling(
					Chain(Receive(), Equal(router.RangeName{
						Low:  0,
						High: 9223372036854775807,
						Term: 0,
					})),
				))
			})
		})
	})

	o.Group("when FileSystem.List returns an error", func() {
		o.BeforeEach(func(t TR) TR {
			t.mockHasher.HashOutput.Hash <- 10000000000000000000
			t.mockHasher.HashOutput.Err <- nil

			t.mockFileSystem.ListOutput.File <- nil
			t.mockFileSystem.ListOutput.Err <- fmt.Errorf("some-error")

			return t
		})

		o.Spec("it returns an error", func(t TR) {
			err := t.r.Write([]byte("some-data"))
			Expect(t, err == nil).To(BeFalse())
		})
	})

	o.Group("hasher returns an error", func() {
		o.BeforeEach(func(t TR) TR {
			close(t.mockFileSystem.ListOutput.File)
			close(t.mockFileSystem.ListOutput.Err)

			t.mockHasher.HashOutput.Hash <- 0
			t.mockHasher.HashOutput.Err <- fmt.Errorf("some-error")
			return t
		})

		o.Spec("it returns an error", func(t TR) {
			err := t.r.Write([]byte("some-data"))
			Expect(t, err == nil).To(BeFalse())
		})
	})
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
