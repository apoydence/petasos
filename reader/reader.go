package reader

import (
	"encoding/json"
	"io"
	"sort"

	"github.com/poy/petasos/router"
)

type Reader interface {
	Read() (data DataPacket, err error)
	Close()
}

type FileSystem interface {
	List() (file []string, err error)
	Reader(name string, startingIndex uint64) (reader Reader, err error)
}

type DataPacket struct {
	Payload  []byte
	Filename string
	Index    uint64
}

type RouteReader struct {
	fs FileSystem
}

func NewRouteReader(fs FileSystem) *RouteReader {
	return &RouteReader{
		fs: fs,
	}
}

func (r *RouteReader) ReadFrom(hash uint64) Reader {
	return newFileReader(hash, r.fs)
}

type fileReader struct {
	hash uint64
	fs   FileSystem

	currentFile Reader
	current     string
	currentIdx  uint64

	history    map[hashRange]bool
	historyIdx map[string]uint64
}

type hashRange struct {
	file string
	r    router.RangeName
}

func newFileReader(hash uint64, fs FileSystem) *fileReader {
	return &fileReader{
		hash:       hash,
		fs:         fs,
		history:    make(map[hashRange]bool),
		historyIdx: make(map[string]uint64),
	}
}

func (r *fileReader) Read() (data DataPacket, err error) {
	for {
		if r.currentFile == nil {
			next, err := r.fetchNextFile()
			if err != nil {
				return DataPacket{}, err
			}

			// Grab the next index if we have one
			idx, ok := r.historyIdx[next.file]
			if ok {
				idx++
			}

			reader, err := r.fs.Reader(next.file, idx)
			if err != nil {
				return DataPacket{}, err
			}
			r.currentFile = reader
		}

		data, err = r.currentFile.Read()
		if err == io.EOF {
			r.currentFile.Close()
			r.currentFile = nil

			continue
		}

		if err != nil {
			return DataPacket{}, err
		}

		r.historyIdx[data.Filename] = data.Index

		return data, nil
	}
}

func (r *fileReader) Close() {
	r.currentFile.Close()
}

func (r *fileReader) fetchNextFile() (hashRange, error) {
	files, err := r.fetchFromRange()
	if err != nil {
		return hashRange{}, err
	}

	if len(files) == 0 {
		return hashRange{}, io.EOF
	}

	file := files[0]
	r.history[file] = true
	return file, nil
}

func (r *fileReader) notInHistory(hr hashRange) bool {
	_, ok := r.history[hr]
	return !ok
}

func (r *fileReader) fetchFromRange() (files []hashRange, err error) {
	ranges, err := r.setupRanges()
	if err != nil {
		return nil, err
	}

	if len(ranges) == 0 {
		return nil, nil
	}

	sort.Sort(hashRanges(ranges))

	var (
		matchedRange []hashRange
		lastRange    hashRange
	)
	for _, hashRange := range ranges {
		if r.hash < hashRange.r.Low || r.hash > hashRange.r.High {
			continue
		}

		if r.notInHistory(hashRange) {
			matchedRange = append(matchedRange, hashRange)
		}

		lastRange = hashRange
	}

	if len(matchedRange) == 0 {
		delete(r.history, lastRange)
	}

	return matchedRange, nil
}

func (r *fileReader) setupRanges() (ranges []hashRange, err error) {
	list, err := r.fs.List()
	if err != nil {
		return nil, err
	}

	for _, file := range list {
		var rn router.RangeName
		err := json.Unmarshal([]byte(file), &rn)
		if err != nil {
			return nil, err
		}

		ranges = append(ranges, hashRange{
			file: file,
			r:    rn,
		})
	}

	return ranges, nil
}

func (r *fileReader) lowHigh(file string) (low, high uint64, err error) {
	var rn router.RangeName
	if err := json.Unmarshal([]byte(file), &rn); err != nil {
		return 0, 0, err
	}

	return rn.Low, rn.High, nil
}

type hashRanges []hashRange

func (s hashRanges) Len() int {
	return len(s)
}

func (s hashRanges) Less(i, j int) bool {
	return s[i].r.Term < s[j].r.Term
}

func (s hashRanges) Swap(i, j int) {
	tmp := s[i]
	s[i] = s[j]
	s[j] = tmp
}
