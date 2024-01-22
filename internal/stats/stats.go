package stats

import (
	"encoding/json"
	"os"
	"sync/atomic"
)

// minCacheLineSize is the minimum cache line size, used to prevent false
// sharing. Smaller values have an insignificant effect on memory usage. Larger
// values help separate values into separate cache lines.
const minCacheLineSize = 128

// Statistics contains various statistics.
type Statistics struct {
	Errors      atomic.Uint64
	_           [minCacheLineSize - 8]byte
	DirEntries  atomic.Uint64
	_           [minCacheLineSize - 8]byte
	TotalBytes  atomic.Uint64
	_           [minCacheLineSize - 8]byte
	FilesOpened atomic.Uint64
	_           [minCacheLineSize - 8]byte
	BytesHashed atomic.Uint64
	_           [minCacheLineSize - 8]byte
}

// Print prints [Statistics] to [os.Stderr].
func (s *Statistics) Print() error {
	errors := s.Errors.Load()
	dirEntries := s.DirEntries.Load()
	filesOpened := s.FilesOpened.Load()
	totalBytes := s.TotalBytes.Load()
	bytesHashed := s.BytesHashed.Load()
	statisticsEncoder := json.NewEncoder(os.Stderr)
	statisticsEncoder.SetIndent("", "  ")

	return statisticsEncoder.Encode(struct {
		Errors             uint64  `json:"errors"`
		DirEntries         uint64  `json:"dirEntries"`
		FilesOpened        uint64  `json:"filesOpened"`
		FilesOpenedPercent float64 `json:"filesOpenedPercent"`
		TotalBytes         uint64  `json:"totalBytes"`
		BytesHashed        uint64  `json:"bytesHashed"`
		BytesHashedPercent float64 `json:"bytesHashedPercent"`
	}{
		Errors:             errors,
		DirEntries:         dirEntries,
		FilesOpened:        filesOpened,
		FilesOpenedPercent: 100 * float64(filesOpened) / max(1, float64(dirEntries)),
		TotalBytes:         totalBytes,
		BytesHashed:        bytesHashed,
		BytesHashedPercent: 100 * float64(bytesHashed) / max(1, float64(totalBytes)),
	})
}
