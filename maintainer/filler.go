package maintainer

import (
	"encoding/json"
	"log"
	"math/rand"
	"sort"
	"time"

	"github.com/apoydence/petasos/router"
)

type Filler struct {
	rangeMetrics RangeMetrics
	fs           FileSystem
	conf         fillerConfig
}

type fillerConfig struct {
	min      uint64
	interval time.Duration
}

type FillerOpts func(c *fillerConfig)

func StartFiller(rangeMetrics RangeMetrics, fs FileSystem, opts ...FillerOpts) *Filler {
	conf := fillerConfig{
		interval: 5 * time.Second,
		min:      3,
	}

	for _, opt := range opts {
		opt(&conf)
	}

	f := &Filler{
		conf:         conf,
		rangeMetrics: rangeMetrics,
		fs:           fs,
	}
	go f.run()

	return f
}

func (f *Filler) run() {
	for range time.Tick(f.conf.interval) {
		ranges, actual, lastTerm, _ := validRanges(f.fs, f.rangeMetrics)
		if uint64(len(actual)) < f.conf.min {
			continue
		}

		sort.Sort(rangeInfos(ranges))

		if gap, foundOne := f.findGap(0, 18446744073709551615, ranges); foundOne {
			f.fillGap(gap, lastTerm)
			continue
		}
	}
}

// findGap takes only valid ranges
func (f *Filler) findGap(start, end uint64, ranges []rangeInfo) (router.RangeName, bool) {
	var gapEnd uint64 = 18446744073709551615
	for _, x := range ranges {
		if x.hashRange.Low == start {
			if x.hashRange.High == 18446744073709551615 {
				return router.RangeName{}, false
			}

			return f.findGap(x.hashRange.High+1, end, ranges)
		}

		if x.hashRange.Low > start && x.hashRange.Low < gapEnd {
			gapEnd = x.hashRange.Low - 1
		}
	}

	return router.RangeName{
		Low:  start,
		High: gapEnd,
		Rand: rand.Int63(),
	}, true
}

func (f *Filler) fillGap(gap router.RangeName, lastTerm uint64) {
	log.Printf("Filling gap (%d - %d)", gap.Low, gap.High)
	defer log.Printf("Done filling gap (%d - %d)...", gap.Low, gap.High)

	gap.Term = lastTerm + 1

	gapName, _ := json.Marshal(gap)

	if err := f.fs.Create(string(gapName)); err != nil {
		log.Printf("Error creating file %s to read only: %s", string(gapName), err)
	}
}
