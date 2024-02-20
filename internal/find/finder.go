package find

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

type Finder struct {
	Roots              []string
	DuplicateThreshold int
	KeepGoing          bool
	Statistics         *Statistics
}

// channelBufferCapacity is the buffer capacity between different components.
// Larger values increase performance by allowing different components to run at
// different speeds, at the expense of memory usage.
const channelBufferCapacity = 1024

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

// concurrentWalkDir is like [fs.WalkDir] except that directories are walked concurrently.
func (f *Finder) concurrentWalkDir(root string, walkDirFunc fs.WalkDirFunc, errCh chan<- error) {
	dirEntries, err := os.ReadDir(root)
	if err != nil {
		errCh <- walkDirFunc(root, nil, err)
		return
	}
	f.Statistics.dirEntries.Add(uint64(len(dirEntries)))
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
			_ = ants.Submit(func() {
				defer wg.Done()
				f.concurrentWalkDir(path, walkDirFunc, errCh)
			})
		}
	}
	wg.Wait()
}

// findRegularFiles walks root and writes all regular files and their sizes to
// regularFilesCh.
func (f *Finder) findRegularFiles(root string, regularFilesCh chan<- pathWithSize, errCh chan<- error) {
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
		f.Statistics.totalBytes.Add(uint64(size))
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
func (f *Finder) findUniquePathsWithSize(uniquePathsWithSizeCh chan<- pathWithSize, regularFilesCh <-chan pathWithSize) {
	allPaths := make(map[pathWithSize]struct{})
	for pathWithSize := range regularFilesCh {
		if _, ok := allPaths[pathWithSize]; !ok {
			allPaths[pathWithSize] = struct{}{}
			uniquePathsWithSizeCh <- pathWithSize
		}
	}
}

// findPathsWithIdenticalSizes reads paths from uniquePathsWithSize and, once
// there are more than threshold paths with the same size, writes them to
// pathsToHashCh.
func (f *Finder) findPathsWithIdenticalSizes(pathsToHashCh chan<- pathWithSize, uniquePathsWithSize <-chan pathWithSize, threshold int) {
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

// hashPaths reads paths from pathsToHashCh, computes their hashes, and writes
// them to pathsWithHashCh.
func (f *Finder) hashPaths(pathsToHashCh <-chan pathWithSize, pathsWithHashCh chan<- pathWithHash, errCh chan<- error) {
	var wg sync.WaitGroup
	for pathWithSize := range pathsToHashCh {
		pathWithSize := pathWithSize
		wg.Add(1)
		_ = ants.Submit(func() {
			pathWithHash, err := f.pathWithHash(pathWithSize)
			if err != nil {
				errCh <- err
			} else {
				pathsWithHashCh <- pathWithHash
			}
			wg.Done()
		})
	}
	wg.Wait()
}

// pathWithHash hashes p and returns a pathWithHash.
func (f *Finder) pathWithHash(p pathWithSize) (pathWithHash, error) {
	hash, err := f.hash(p)
	if err != nil {
		return pathWithHash{}, err
	}
	pathWithHash := pathWithHash{
		path: p.path,
		hash: hash,
	}
	return pathWithHash, nil
}

// hash returns p's hash.
func (f *Finder) hash(p pathWithSize) (xxh3.Uint128, error) {
	if p.size == 0 {
		return emptyHash, nil
	}
	file, err := os.Open(p.path)
	if err != nil {
		return xxh3.Uint128{}, err
	}
	f.Statistics.filesOpened.Add(1)
	defer file.Close()
	hash := xxh3.New()
	written, err := io.Copy(hash, file)
	if err != nil {
		return xxh3.Uint128{}, err
	}
	f.Statistics.bytesHashed.Add(uint64(written))
	return hash.Sum128(), nil
}

func (f *Finder) FindDuplicates() (map[string][]string, error) {
	defer ants.Release()

	var errHandler func(error) error
	if f.KeepGoing {
		errHandler = func(err error) error {
			if err != nil {
				f.Statistics.errors.Add(1)
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
		for _, root := range f.Roots {
			root := root
			wg.Add(1)
			_ = ants.Submit(func() {
				f.findRegularFiles(root, regularFilesCh, errCh)
				wg.Done()
			})
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
		f.findPathsWithIdenticalSizes(pathsToHashCh, uniquePathsWithSizeCh, f.DuplicateThreshold)
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
			if len(paths) >= f.DuplicateThreshold {
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
