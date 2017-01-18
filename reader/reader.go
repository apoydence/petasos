package reader

import (
	"encoding/json"
	"io"
	"sort"

	"github.com/apoydence/petasos/router"
)

type Reader interface {
	Read() (data []byte, err error)
}

type FileSystem interface {
	List() (file []string, err error)
	Reader(name string) (reader Reader, err error)
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
	history     map[hashRange]bool
}

type hashRange struct {
	file string
	r    router.RangeName
}

func newFileReader(hash uint64, fs FileSystem) *fileReader {
	return &fileReader{
		hash:    hash,
		fs:      fs,
		history: make(map[hashRange]bool),
	}
}

func (r *fileReader) Read() (data []byte, err error) {
	if r.currentFile == nil {
		next, err := r.fetchNextFile()
		if err != nil {
			return nil, err
		}

		reader, err := r.fs.Reader(next.file)
		if err != nil {
			return nil, err
		}
		r.currentFile = reader
	}

	data, err = r.currentFile.Read()
	if err == io.EOF {
		r.currentFile = nil
		return r.Read()
	}

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *fileReader) fetchNextFile() (hashRange, error) {
	files, err := r.fetchFromRange()
	if err != nil {
		return hashRange{}, err
	}

	sort.Sort(hashRanges(files))

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

	var matchedRange []hashRange
	for _, hashRange := range ranges {
		if r.hash >= hashRange.r.Low && r.hash <= hashRange.r.High && r.notInHistory(hashRange) {
			matchedRange = append(matchedRange, hashRange)
		}
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
