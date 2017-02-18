package router

import (
	"encoding/json"
	"fmt"
	"log"
)

type Writer interface {
	Write(data []byte) (err error)
	Close()
}

type FileSystem interface {
	List() (file []string, err error)
	Writer(name string) (writer Writer, err error)
}

type Hasher interface {
	Hash(data []byte) (hash uint64, err error)
}

type MetricsCounter interface {
	IncSuccess(name RangeName)
	IncFailure(name RangeName)
}

type writerInfo struct {
	writer    Writer
	rangeName RangeName
}

type Router struct {
	fs             FileSystem
	hasher         Hasher
	metricsCounter MetricsCounter

	ranges  []hashRange
	writers map[uint64]writerInfo
}

type hashRange struct {
	file string
	r    RangeName
}

func New(fs FileSystem, hasher Hasher, metricsCounter MetricsCounter) *Router {
	return &Router{
		fs:             fs,
		hasher:         hasher,
		metricsCounter: metricsCounter,
	}
}

func (r *Router) Write(data []byte) (err error) {
	hash, err := r.hasher.Hash(data)
	if err != nil {
		return err
	}

	writer, err := r.fetchWriter(hash)
	if err != nil {
		r.writeFailure()
		return err
	}

	if err = writer.writer.Write(data); err != nil {
		r.writeFailure()
		r.metricsCounter.IncFailure(writer.rangeName)

		return err
	}

	r.metricsCounter.IncSuccess(writer.rangeName)

	return nil
}

func (r *Router) writeFailure() {
	for _, w := range r.writers {
		w.writer.Close()
	}

	r.ranges = nil
	r.writers = nil
}

func (r *Router) fetchWriter(hash uint64) (writer writerInfo, err error) {
	writer, ok := r.writers[hash]
	if ok {
		return writer, nil
	}

	file, err := r.fetchFromRange(hash)
	if err != nil {
		return writerInfo{}, err
	}

	w, err := r.fs.Writer(file)
	if err != nil {
		return writerInfo{}, err
	}

	var rangeName RangeName
	if err := json.Unmarshal([]byte(file), &rangeName); err != nil {
		return writerInfo{}, err
	}

	writer = writerInfo{
		writer:    w,
		rangeName: rangeName,
	}
	r.writers[hash] = writer

	return writer, nil
}

func (r *Router) fetchFromRange(hash uint64) (file string, err error) {
	if r.ranges == nil {
		r.ranges, err = r.setupRanges()
		if err != nil {
			return "", err
		}
		r.writers = make(map[uint64]writerInfo)
	}

	var matchedRange hashRange
	for _, hashRange := range r.ranges {
		if hash >= hashRange.r.Low && hash <= hashRange.r.High && matchedRange.r.Term <= hashRange.r.Term {
			matchedRange = hashRange
		}
	}

	if matchedRange.file == "" {
		return "", fmt.Errorf("%d does not have a home", hash)
	}

	return matchedRange.file, nil
}

func (r *Router) setupRanges() (ranges []hashRange, err error) {
	list, err := r.fs.List()
	if err != nil {
		return nil, err
	}

	for _, file := range list {
		var rn RangeName
		err := json.Unmarshal([]byte(file), &rn)
		if err != nil {
			log.Printf("Non-petasos range: %s", file)
			continue
		}

		ranges = append(ranges, hashRange{
			file: file,
			r:    rn,
		})
	}

	if len(ranges) == 0 {
		return nil, fmt.Errorf("empty ranges")
	}

	return ranges, nil
}

func (r *Router) lowHigh(file string) (low, high uint64, err error) {
	var rn RangeName
	if err := json.Unmarshal([]byte(file), &rn); err != nil {
		return 0, 0, err
	}

	return rn.Low, rn.High, nil
}
