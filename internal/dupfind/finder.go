package dupfind

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/panjf2000/ants/v2"
	"github.com/zeebo/xxh3"
)

// channelBufferCapacity is the buffer capacity between different components.
// Larger values increase performance by allowing different components to run at
// different speeds, at the expense of memory usage.
const channelBufferCapacity = 1024

type DupFinder struct {
	keepGoing  bool
	roots      []string
	threshold  int
	statistics Statistics
}

// An Option sets an option on a [*DupFinder].
type Option func(*DupFinder)

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

// WithKeepGoing sets whether to keep going on errors.
func WithKeepGoing(keepGoing bool) Option {
	return func(f *DupFinder) {
		f.keepGoing = keepGoing
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
		threshold: 2,
	}
	for _, option := range options {
		option(f)
	}
	return f
}

func (f *DupFinder) FindDuplicates() (map[string][]string, error) {
	defer ants.Release()

	var errHandler func(error) error
	if f.keepGoing {
		errHandler = func(err error) error {
			if err != nil {
				f.statistics.errors.Add(1)
				fmt.Fprintln(os.Stderr, err)
			}
			return nil
		}
	} else {
		errHandler = func(err error) error {
			return err
		}
	}

	errCh := make(chan error, channelBufferCapacity)
	defer close(errCh)

	// Generate paths with size.
	regularFilesCh := make(chan pathWithSize, channelBufferCapacity)
	go func() {
		defer close(regularFilesCh)
		var wg sync.WaitGroup
		for _, root := range f.roots {
			root := root
			wg.Add(1)
			if err := ants.Submit(func() {
				defer wg.Done()
				f.findRegularFiles(root, regularFilesCh, errCh)
			}); err != nil {
				errCh <- err
			}
		}
		wg.Wait()
	}()

	// Generate unique paths with size.
	uniquePathsWithSizeCh := make(chan pathWithSize, channelBufferCapacity)
	go func() {
		defer close(uniquePathsWithSizeCh)
		f.findUniquePathsWithSize(uniquePathsWithSizeCh, regularFilesCh)
	}()

	// Generate paths with size to hash.
	pathsToHashCh := make(chan pathWithSize, channelBufferCapacity)
	go func() {
		defer close(pathsToHashCh)
		f.findPathsWithIdenticalSizes(pathsToHashCh, uniquePathsWithSizeCh, f.threshold)
	}()

	// Generate paths with hashes.
	pathsWithHashCh := make(chan pathWithHash, channelBufferCapacity)
	go func() {
		defer close(pathsWithHashCh)
		f.hashPaths(pathsToHashCh, pathsWithHashCh, errCh)
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
			if handledErr := errHandler(err); handledErr != nil {
				return nil, handledErr
			}
		case result := <-resultCh:
			return result, nil
		}
	}
}

func (f *DupFinder) Statistics() *Statistics {
	return &f.statistics
}

// concurrentWalkDir is like [fs.WalkDir] except that directories are walked concurrently.
func (f *DupFinder) concurrentWalkDir(root string, walkDirFunc fs.WalkDirFunc, errCh chan<- error) {
	dirEntries, err := os.ReadDir(root)
	if err != nil {
		errCh <- walkDirFunc(root, nil, err)
		return
	}
	f.statistics.dirEntries.Add(uint64(len(dirEntries)))
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
			if err := ants.Submit(func() {
				defer wg.Done()
				f.concurrentWalkDir(path, walkDirFunc, errCh)
			}); err != nil {
				errCh <- err
			}
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
		f.statistics.totalBytes.Add(uint64(size))
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
	f.statistics.bytesHashed.Add(uint64(written))
	return hash.Sum128(), nil
}

// hashPaths reads paths from pathsToHashCh, computes their hashes, and writes
// them to pathsWithHashCh.
func (f *DupFinder) hashPaths(pathsToHashCh <-chan pathWithSize, pathsWithHashCh chan<- pathWithHash, errCh chan<- error) {
	var wg sync.WaitGroup
	for pathWithSize := range pathsToHashCh {
		pathWithSize := pathWithSize
		wg.Add(1)
		if err := ants.Submit(func() {
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
		}); err != nil {
			errCh <- err
		}
	}
	wg.Wait()
}