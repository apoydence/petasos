package maintainer

import (
	"encoding/json"
	"log"
	"sort"
	"time"

	"github.com/apoydence/petasos/router"
)

type RangeMetrics interface {
	Metrics(file string) (metric router.Metric, err error)
}

type FileSystem interface {
	Create(file string) (err error)
	List() (file []string, err error)
}

type Balancer struct {
	rangeMetrics RangeMetrics
	fs           FileSystem
	conf         balancerConfig
}

type balancerConfig struct {
	interval       time.Duration
	maxPerInterval uint64
	minPerInterval uint64
	min, max       uint64
}

type BalancerOpts func(c *balancerConfig)

func StartBalancer(rangeMetrics RangeMetrics, fs FileSystem, opts ...BalancerOpts) {
	conf := balancerConfig{
		interval:       5 * time.Second,
		maxPerInterval: 2500,
		minPerInterval: 20,
		max:            100,
		min:            3,
	}

	for _, opt := range opts {
		opt(&conf)
	}

	if conf.min > conf.max || conf.min == 0 || conf.max == 0 {
		log.Panicf("Invalid config: %+v", conf)
	}

	b := &Balancer{
		rangeMetrics: rangeMetrics,
		fs:           fs,
		conf:         conf,
	}

	go b.run()
}

func (b *Balancer) run() {
	for range time.Tick(b.conf.interval) {
		ranges, lastTerm, ok := validRanges(b.fs, b.rangeMetrics)
		if !ok {
			continue
		}

		if len(ranges) == 0 {
			b.seedRanges()
			continue
		}

		sort.Sort(rangeInfos(ranges))

		last := ranges[len(ranges)-1]
		if last.writeCount > b.conf.maxPerInterval && uint64(len(ranges)) < b.conf.max {
			b.splitRange(last, lastTerm)
			continue
		}

		first := ranges[0]
		if first.writeCount < b.conf.minPerInterval && uint64(len(ranges)) > b.conf.min {
			b.combineRange(first, ranges[1], lastTerm)
			continue
		}
	}
}

func (b *Balancer) seedRanges() {
	log.Print("Seeding ranges...")
	defer log.Print("Done seeding ranges.")

	width := 18446744073709551615 / b.conf.min

	for i := uint64(0); i < b.conf.min; i++ {
		newRange := router.RangeName{
			Term: i,
			Low:  i*width + 1,
			High: (i + 1) * width,
		}

		if i == 0 {
			newRange.Low = 0
		}

		if i == b.conf.min-1 {
			newRange.High = 18446744073709551615
		}

		rangeName, _ := json.Marshal(newRange)

		if err := b.fs.Create(string(rangeName)); err != nil {
			log.Printf("Error creating file %s: %s", string(rangeName), err)
		}
	}
}

func (b *Balancer) combineRange(first, next rangeInfo, lastTerm uint64) {
	log.Printf("Combining %s and %s...", first.file, next.file)
	defer log.Printf("Done combining %s and %s.", first.file, next.file)

	min := first.hashRange.Low
	if min > next.hashRange.Low {
		min = next.hashRange.Low
	}

	max := first.hashRange.High
	if max < next.hashRange.High {
		max = next.hashRange.High
	}

	combined := router.RangeName{
		Term: lastTerm + 1,
		Low:  min,
		High: max,
	}

	combinedName, _ := json.Marshal(combined)

	if err := b.fs.Create(string(combinedName)); err != nil {
		log.Printf("Error creating file %s: %s", string(combinedName), err)
	}
}

func (b *Balancer) splitRange(last rangeInfo, lastTerm uint64) {
	log.Printf("Splitting %s...", last.file)
	defer log.Printf("Done splitting %s.", last.file)

	middle := (last.hashRange.High-last.hashRange.Low)/2 + last.hashRange.Low
	low := router.RangeName{
		Term: lastTerm + 1,
		Low:  last.hashRange.Low,
		High: middle,
	}

	high := router.RangeName{
		Term: lastTerm + 2,
		Low:  middle + 1,
		High: last.hashRange.High,
	}

	lowName, _ := json.Marshal(low)
	highName, _ := json.Marshal(high)

	if err := b.fs.Create(string(lowName)); err != nil {
		log.Printf("Error creating file %s to read only: %s", string(lowName), err)
	}

	if err := b.fs.Create(string(highName)); err != nil {
		log.Printf("Error creating file %s to read only: %s", string(highName), err)
	}
}

func validRanges(fs FileSystem, rangeMetrics RangeMetrics) (ranges []rangeInfo, lastTerm uint64, ok bool) {
	list, err := fs.List()
	if err != nil {
		log.Printf("Failed to list files: %s", err)
		return nil, 0, false
	}

	for _, file := range list {
		var rn router.RangeName
		if err = json.Unmarshal([]byte(file), &rn); err != nil {
			log.Printf("Unable to unmarshal file name %s: %s", file, err)
			continue
		}

		if lastTerm < rn.Term {
			lastTerm = rn.Term
		}

		metric, err := rangeMetrics.Metrics(file)
		if err != nil {
			log.Printf("Failed to fetch metrics for %s: %s", file, err)
			continue
		}

		if metric.ErrCount >= 5 {
			continue
		}

		ranges = append(ranges, rangeInfo{
			file:       file,
			writeCount: metric.WriteCount,
			hashRange:  rn,
		})
	}

	ranges = removeOverlaps(ranges)

	return ranges, lastTerm, true
}

func removeOverlaps(ranges []rangeInfo) (result []rangeInfo) {
	for i, x := range ranges {
		for j, y := range ranges {
			if i == j || !overlap(x.hashRange, y.hashRange) {
				continue
			}

			removeIdx := i
			if x.hashRange.Term > y.hashRange.Term {
				removeIdx = j
			}

			return removeOverlaps(append(ranges[:removeIdx], ranges[removeIdx+1:]...))
		}
	}

	return ranges
}

func overlap(x, y router.RangeName) bool {
	return (x.Low >= y.Low && x.Low < y.High) || (y.Low >= x.Low && y.Low < x.High)
}

type rangeInfo struct {
	file       string
	writeCount uint64
	hashRange  router.RangeName
}

type rangeInfos []rangeInfo

func (r rangeInfos) Len() int {
	return len(r)
}

func (r rangeInfos) Less(i, j int) bool {
	return r[i].writeCount < r[j].writeCount
}

func (r rangeInfos) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]
	r[j] = tmp
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
