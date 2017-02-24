//go:generate hel

package reader_test

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"github.com/apoydence/eachers/testhelpers"
	"github.com/apoydence/onpar"
	. "github.com/apoydence/onpar/expect"
	. "github.com/apoydence/onpar/matchers"
	"github.com/apoydence/petasos/reader"
	"github.com/apoydence/petasos/router"
)

type TR struct {
	*testing.T

	mockFileSystem *mockFileSystem
	mockReader     *mockReader

	r *reader.RouteReader
}

func TestReader(t *testing.T) {
	t.Parallel()
	o := onpar.New()
	defer o.Run(t)

	o.BeforeEach(func(t *testing.T) TR {
		mockFileSystem := newMockFileSystem()
		mockReader := newMockReader()

		return TR{
			T:              t,
			mockFileSystem: mockFileSystem,
			mockReader:     mockReader,
			r:              reader.NewRouteReader(mockFileSystem),
		}
	})

	o.Group("when there are multiple terms for a hash", func() {
		o.BeforeEach(func(t TR) TR {
			testhelpers.AlwaysReturn(t.mockFileSystem.ListOutput.File, []string{
				buildRangeName(0, 9223372036854775807, 0),
				buildRangeName(9223372036854775808, 10000000000000000000, 0),
				buildRangeName(10000000000000000001, 18446744073709551615, 1),
				buildRangeName(9223372036854775808, 18446744073709551615, 2),
			})
			close(t.mockFileSystem.ListOutput.Err)

			testhelpers.AlwaysReturn(t.mockFileSystem.ReaderOutput.Reader, t.mockReader)
			close(t.mockFileSystem.ReaderOutput.Err)

			return t
		})

		o.Spec("it reads from each file", func(t TR) {
			r := t.r.ReadFrom(10000000000000000000)

			t.mockReader.ReadOutput.Data <- reader.DataPacket{Payload: []byte("some-data-0")}
			t.mockReader.ReadOutput.Err <- nil

			t.mockReader.ReadOutput.Data <- reader.DataPacket{}
			t.mockReader.ReadOutput.Err <- io.EOF

			t.mockReader.ReadOutput.Data <- reader.DataPacket{Payload: []byte("some-data-1")}
			t.mockReader.ReadOutput.Err <- nil

			t.mockReader.ReadOutput.Data <- reader.DataPacket{}
			t.mockReader.ReadOutput.Err <- io.EOF

			for i := 0; i < 2; i++ {
				data, err := r.Read()
				Expect(t, err == nil).To(BeTrue())
				Expect(t, data.Payload).To(Equal([]byte(fmt.Sprintf("some-data-%d", i))))
			}

			Expect(t, t.mockFileSystem.ReaderInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":9223372036854775808,"High":10000000000000000000,"Term":0,"Rand":0}`)),
			)
			Expect(t, t.mockFileSystem.ReaderInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":9223372036854775808,"High":18446744073709551615,"Term":2,"Rand":0}`)),
			)
		})

		o.Spec("it continues to read from the last range", func(t TR) {
			testhelpers.AlwaysReturn(t.mockReader.ReadOutput.Err, io.EOF)
			close(t.mockReader.ReadOutput.Data)

			reader := t.r.ReadFrom(10000000000000000000)

			reader.Read()
			reader.Read()
			reader.Read()
			reader.Read()

			Expect(t, t.mockFileSystem.ReaderInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":9223372036854775808,"High":10000000000000000000,"Term":0,"Rand":0}`)),
			)
			Expect(t, t.mockFileSystem.ReaderInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":9223372036854775808,"High":18446744073709551615,"Term":2,"Rand":0}`)),
			)
			Expect(t, t.mockFileSystem.ReaderInput.Name).To(
				Chain(Receive(), MatchJSON(`{"Low":9223372036854775808,"High":18446744073709551615,"Term":2,"Rand":0}`)),
			)
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
