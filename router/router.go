package router

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Writer interface {
	Write(data []byte) (err error)
}

type FileSystem interface {
	List() (file []string, err error)
	Writer(name string) (writer Writer, err error)
}

type Hasher interface {
	Hash(data []byte) (hash uint64, err error)
}

type Router struct {
	fs     FileSystem
	hasher Hasher

	mu      sync.Mutex
	ranges  []hashRange
	writers map[uint64]Writer
}

type hashRange struct {
	file      string
	low, high uint64
}

func New(fs FileSystem, hasher Hasher) *Router {
	return &Router{
		fs:     fs,
		hasher: hasher,
	}
}

func (r *Router) Write(data []byte) (err error) {
	hash, err := r.hasher.Hash(data)
	if err != nil {
		return err
	}

	writer, err := r.fetchWriter(hash)
	if err != nil {
		return err
	}

	if err = writer.Write(data); err != nil {
		r.writeFailure()
		return err
	}

	return nil
}

func (r *Router) writeFailure() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ranges = nil
	r.writers = nil
}

func (r *Router) fetchWriter(hash uint64) (writer Writer, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	writer, ok := r.writers[hash]
	if ok {
		return writer, nil
	}

	file, err := r.fetchFromRange(hash)
	if err != nil {
		return nil, err
	}

	writer, err = r.fs.Writer(file)
	if err != nil {
		return nil, err
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
		r.writers = make(map[uint64]Writer)
	}

	for _, hashRange := range r.ranges {
		if hash >= hashRange.low && hash <= hashRange.high {
			return hashRange.file, nil
		}
	}

	return "", fmt.Errorf("%d does not have a home", hash)
}

func (r *Router) setupRanges() (ranges []hashRange, err error) {
	list, err := r.fs.List()
	if err != nil {
		return nil, err
	}

	for _, file := range list {
		low, high, err := r.lowHigh(file)
		if err != nil {
			return nil, err
		}

		ranges = append(ranges, hashRange{
			file: file,
			low:  low,
			high: high,
		})
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
