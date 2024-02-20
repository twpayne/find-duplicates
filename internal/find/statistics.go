package find

import (
	"encoding/json"
	"sync/atomic"
)

// minCacheLineSize is the minimum cache line size, used to prevent false
// sharing. Smaller values have an insignificant effect on memory usage. Larger
// values help separate values into separate cache lines.
const minCacheLineSize = 128

// Statistics contains various statistics.
type Statistics struct {
	errors      atomic.Uint64
	_           [minCacheLineSize - 8]byte
	dirEntries  atomic.Uint64
	_           [minCacheLineSize - 8]byte
	totalBytes  atomic.Uint64
	_           [minCacheLineSize - 8]byte
	filesOpened atomic.Uint64
	_           [minCacheLineSize - 8]byte
	bytesHashed atomic.Uint64
	_           [minCacheLineSize - 8]byte
}

func (s *Statistics) MarshalJSON() ([]byte, error) {
	errors := s.errors.Load()
	dirEntries := s.dirEntries.Load()
	filesOpened := s.filesOpened.Load()
	totalBytes := s.totalBytes.Load()
	bytesHashed := s.bytesHashed.Load()

	return json.Marshal(struct {
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
