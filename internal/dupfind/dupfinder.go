package dupfind

// FIXME on slow network filesystems with lots of files this code exceeds the 10000-thread limit
// FIXME when keeping going despite errors this code can panic with "write to closed channel" as DupFinder.FindDuplicates closes channels while goroutines are still running

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/twpayne/go-heap"
	"github.com/zeebo/xxh3"
	"golang.org/x/sys/cpu"
)

// A DupFinder finds duplicate files.
type DupFinder struct {
	channelBufferCapacity int
	errorHandler          func(error) error
	roots                 []string
	threshold             int
	statistics            struct {
		errors      atomic.Uint64
		_           cpu.CacheLinePad
		dirEntries  atomic.Uint64
		_           cpu.CacheLinePad
		files       atomic.Uint64
		_           cpu.CacheLinePad
		totalBytes  atomic.Uint64
		_           cpu.CacheLinePad
		filesOpened atomic.Uint64
		_           cpu.CacheLinePad
		bytesHashed atomic.Uint64
		_           cpu.CacheLinePad
		uniqueSizes atomic.Uint64
		_           cpu.CacheLinePad
	}
}

// An Option sets an option on a [*DupFinder].
type Option func(*DupFinder)

// Statistics contains various statistics.
type Statistics struct {
	Errors             uint64  `json:"errors"`
	DirEntries         uint64  `json:"dirEntries"`
	Files              uint64  `json:"files"`
	FilesOpened        uint64  `json:"filesOpened"`
	FilesOpenedPercent float64 `json:"filesOpenedPercent"`
	TotalBytes         uint64  `json:"totalBytes"`
	BytesHashed        uint64  `json:"bytesHashed"`
	BytesHashedPercent float64 `json:"bytesHashedPercent"`
	UniqueSizes        uint64  `json:"uniqueSizes"`
}

// A pathWithSize contains a path to a regular file and its size.
type pathWithSize struct {
	path string
	size int64
}

// A pathWithHash contains a path to a regular file and its hash.
type pathWithHash struct {
	path string
	hash xxh3.Uint128
}

// emptyHash is the hash of the empty file.
var emptyHash = xxh3.New().Sum128()

// WithChannelBufferCapacity sets the buffer capacity between different
// components. Larger values increase performance by allowing different
// components to run at different speeds, at the expense of memory usage.
func WithChannelBufferCapacity(channelBufferCapacity int) Option {
	return func(f *DupFinder) {
		f.channelBufferCapacity = channelBufferCapacity
	}
}

func WithErrorHandler(errorHandler func(error) error) Option {
	return func(f *DupFinder) {
		f.errorHandler = errorHandler
	}
}

// WithRoots sets the roots.
func WithRoots(roots ...string) Option {
	return func(f *DupFinder) {
		f.roots = append(f.roots, roots...)
	}
}

// WithThreshold sets the threshold.
func WithThreshold(threshold int) Option {
	return func(f *DupFinder) {
		f.threshold = threshold
	}
}

// NewDupFinder returns a new [*DupFinder] with the given options.
func NewDupFinder(options ...Option) *DupFinder {
	f := &DupFinder{
		channelBufferCapacity: 1024,
		errorHandler:          func(err error) error { return err },
		threshold:             2,
	}
	for _, option := range options {
		option(f)
	}
	return f
}

func (f *DupFinder) FindDuplicates(ctx context.Context) (map[string][]string, error) {
	errCh := make(chan error, f.channelBufferCapacity)
	defer close(errCh)

	// Generate paths with size.
	regularFilesCh := make(chan pathWithSize, f.channelBufferCapacity)
	go func() {
		defer close(regularFilesCh)
		var wg sync.WaitGroup
		for _, root := range f.roots {
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.findRegularFiles(root, regularFilesCh, errCh)
			}()
		}
		wg.Wait()
	}()

	// Generate unique paths with size.
	uniquePathsWithSizeCh := make(chan pathWithSize, f.channelBufferCapacity)
	go func() {
		defer close(uniquePathsWithSizeCh)
		f.findUniquePathsWithSize(uniquePathsWithSizeCh, regularFilesCh)
	}()

	// Generate paths with size to hash.
	pathsToHashCh := make(chan pathWithSize, f.channelBufferCapacity)
	go func() {
		defer close(pathsToHashCh)
		f.findPathsWithIdenticalSizes(pathsToHashCh, uniquePathsWithSizeCh, f.threshold)
	}()

	// Prioritize larger files. Use an un-buffered channel so that we accumulate
	// as many pathWithSizes as possible before sending the path with the
	// largest size.
	prioritizedPathsToHashCh := heap.PriorityChannel(ctx, pathsToHashCh, func(a, b pathWithSize) bool {
		return a.size > b.size
	})

	// Generate paths with hashes.
	pathsWithHashCh := make(chan pathWithHash, f.channelBufferCapacity)
	go func() {
		defer close(pathsWithHashCh)
		f.hashPaths(pathsWithHashCh, prioritizedPathsToHashCh, errCh)
	}()

	// Accumulate paths by hash.
	pathsByHash := make(map[xxh3.Uint128][]string)
	resultCh := make(chan map[string][]string)
	go func() {
		defer close(resultCh)

		for pathWithHash := range pathsWithHashCh {
			pathsByHash[pathWithHash.hash] = append(pathsByHash[pathWithHash.hash], pathWithHash.path)
		}

		// Find all duplicates, indexed by hex string of their checksum.
		result := make(map[string][]string, len(pathsByHash))
		for hash, paths := range pathsByHash {
			if len(paths) >= f.threshold {
				bytes := hash.Bytes()
				key := hex.EncodeToString(bytes[:])
				slices.Sort(paths)
				result[key] = paths
			}
		}
		resultCh <- result
	}()

	// Wait for all goroutines to finish.
	for {
		select {
		case err := <-errCh:
			f.statistics.errors.Add(1)
			if handledErr := f.errorHandler(err); handledErr != nil {
				return nil, handledErr
			}
		case result := <-resultCh:
			return result, nil
		}
	}
}

func (f *DupFinder) Statistics() *Statistics {
	errors := f.statistics.errors.Load()
	dirEntries := f.statistics.dirEntries.Load()
	files := f.statistics.files.Load()
	filesOpened := f.statistics.filesOpened.Load()
	totalBytes := f.statistics.totalBytes.Load()
	bytesHashed := f.statistics.bytesHashed.Load()
	uniqueSizes := f.statistics.uniqueSizes.Load()

	return &Statistics{
		Errors:             errors,
		DirEntries:         dirEntries,
		Files:              files,
		FilesOpened:        filesOpened,
		FilesOpenedPercent: 100 * float64(filesOpened) / max(1, float64(files)),
		TotalBytes:         totalBytes,
		BytesHashed:        bytesHashed,
		BytesHashedPercent: 100 * float64(bytesHashed) / max(1, float64(totalBytes)),
		UniqueSizes:        uniqueSizes,
	}
}

// concurrentWalkDir is like [fs.WalkDir] except that directories are walked concurrently.
func (f *DupFinder) concurrentWalkDir(root string, walkDirFunc fs.WalkDirFunc, errCh chan<- error) {
	dirEntries, err := os.ReadDir(root)
	if err != nil {
		errCh <- walkDirFunc(root, nil, err)
		return
	}
	f.statistics.dirEntries.Add(uint64(len(dirEntries)))
	files := 0
	for _, dirEntry := range dirEntries {
		if dirEntry.Type().IsRegular() {
			files++
		}
	}
	f.statistics.files.Add(uint64(files)) //nolint:gosec
	var wg sync.WaitGroup
FOR:
	for _, dirEntry := range dirEntries {
		path := filepath.Join(root, dirEntry.Name())
		switch err := walkDirFunc(path, dirEntry, nil); {
		case errors.Is(err, fs.SkipAll):
			break FOR
		case dirEntry.IsDir() && errors.Is(err, fs.SkipDir):
			// Skip directory.
		case err != nil:
			errCh <- err
			return
		case dirEntry.IsDir():
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.concurrentWalkDir(path, walkDirFunc, errCh)
			}()
		}
	}
	wg.Wait()
}

// findPathsWithIdenticalSizes reads paths from uniquePathsWithSize and, once
// there are more than threshold paths with the same size, writes them to
// pathsToHashCh.
func (f *DupFinder) findPathsWithIdenticalSizes(pathsToHashCh chan<- pathWithSize, uniquePathsWithSize <-chan pathWithSize, threshold int) {
	allPathsBySize := make(map[int64][]pathWithSize)
	for pathWithSize := range uniquePathsWithSize {
		pathsBySize := append(allPathsBySize[pathWithSize.size], pathWithSize) //nolint:gocritic
		allPathsBySize[pathWithSize.size] = pathsBySize
		if len(pathsBySize) == threshold {
			for _, p := range pathsBySize {
				pathsToHashCh <- p
			}
		} else if len(pathsBySize) > threshold {
			pathsToHashCh <- pathWithSize
		}
	}
	f.statistics.uniqueSizes.Add(uint64(len(allPathsBySize)))
}

// findRegularFiles walks root and writes all regular files and their sizes to
// regularFilesCh.
func (f *DupFinder) findRegularFiles(root string, regularFilesCh chan<- pathWithSize, errCh chan<- error) {
	walkDirFunc := func(path string, dirEntry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if dirEntry.Type() != 0 {
			return nil
		}
		fileInfo, err := dirEntry.Info()
		if err != nil {
			return err
		}
		size := fileInfo.Size()
		f.statistics.totalBytes.Add(uint64(size)) //nolint:gosec
		regularFilesCh <- pathWithSize{
			path: path,
			size: size,
		}
		return nil
	}
	f.concurrentWalkDir(root, walkDirFunc, errCh)
}

// findUniquePathsWithSize reads paths from regularFilesCh and not-seen-before
// ones to uniquePathsWithSize.
func (f *DupFinder) findUniquePathsWithSize(uniquePathsWithSizeCh chan<- pathWithSize, regularFilesCh <-chan pathWithSize) {
	allPaths := make(map[pathWithSize]struct{})
	for pathWithSize := range regularFilesCh {
		if _, ok := allPaths[pathWithSize]; !ok {
			allPaths[pathWithSize] = struct{}{}
			uniquePathsWithSizeCh <- pathWithSize
		}
	}
}

// hashPath returns p's hash.
func (f *DupFinder) hashPath(p pathWithSize) (xxh3.Uint128, error) {
	if p.size == 0 {
		return emptyHash, nil
	}
	f.statistics.filesOpened.Add(1)
	file, err := os.Open(p.path)
	if err != nil {
		return xxh3.Uint128{}, err
	}
	defer file.Close()
	hash := xxh3.New()
	written, err := io.Copy(hash, file)
	if err != nil {
		return xxh3.Uint128{}, err
	}
	f.statistics.bytesHashed.Add(uint64(written)) //nolint:gosec
	return hash.Sum128(), nil
}

// hashPaths reads paths from pathsToHashCh, computes their hashes, and writes
// them to pathsWithHashCh.
func (f *DupFinder) hashPaths(pathsWithHashCh chan<- pathWithHash, pathsToHashCh <-chan pathWithSize, errCh chan<- error) {
	var wg sync.WaitGroup
	for pathWithSize := range pathsToHashCh {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hash, err := f.hashPath(pathWithSize)
			if err != nil {
				errCh <- err
			} else {
				pathsWithHashCh <- pathWithHash{
					path: pathWithSize.path,
					hash: hash,
				}
			}
		}()
	}
	wg.Wait()
}
