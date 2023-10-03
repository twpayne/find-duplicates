// find-duplicates finds duplicate files, concurrently.
package main

// FIXME handle multiple roots (arguments)
// FIXME de-duplicate filenames in different roots
// FIXME factor out core functionality into a separate package

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
)

// channelBufferCapacity is the buffer capacity between different components.
// Larger values increase performance by allowing different components to run at
// different speeds, at the expense of memory usage.
const channelBufferCapacity = 1024

// A hash is a SHA256 hash.
type hash = [sha256.Size]byte

// A pathWithSize contains a path to a regular file and its size.
type pathWithSize struct {
	path string
	size int64
}

// A pathWithHash contains a path to a regular file and its SHA256 hash.
type pathWithHash struct {
	path string
	hash hash
}

// sha256SumZero is the SHA256 sum of the empty file.
var sha256SumZero = sha256.New().Sum(nil)

// statistics contains various statistics.
var statistics struct {
	dirEntries  atomic.Uint64
	_           [56]byte
	totalBytes  atomic.Uint64
	_           [56]byte
	filesOpened atomic.Uint64
	_           [56]byte
	bytesHashed atomic.Uint64
	_           [56]byte
}

// concurrentWalkDir is like io/fs.WalkDir except that directories are walked concurrently.
func concurrentWalkDir(ctx context.Context, root string, walkDirFunc fs.WalkDirFunc) error {
	dirEntries, err := os.ReadDir(root)
	if err != nil {
		return walkDirFunc(root, nil, err)
	}
	statistics.dirEntries.Add(uint64(len(dirEntries)))
	errGroup, ctx := errgroup.WithContext(ctx)
FOR:
	for _, dirEntry := range dirEntries {
		path := filepath.Join(root, dirEntry.Name())
		switch err := walkDirFunc(path, dirEntry, nil); {
		case errors.Is(err, fs.SkipAll):
			break FOR
		case dirEntry.IsDir() && errors.Is(err, fs.SkipDir):
			// Skip directory.
		case err != nil:
			return err
		case dirEntry.IsDir():
			errGroup.Go(func() error {
				return concurrentWalkDir(ctx, path, walkDirFunc)
			})
		}
	}
	return errGroup.Wait()
}

// findRegularFiles walks root and writes all regular files and their sizes to
// regularFilesCh.
func findRegularFiles(ctx context.Context, regularFilesCh chan<- pathWithSize, root string) error {
	return concurrentWalkDir(ctx, root, func(path string, dirEntry fs.DirEntry, err error) error {
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
		statistics.totalBytes.Add(uint64(size))
		regularFilesCh <- pathWithSize{
			path: path,
			size: size,
		}
		return nil
	})
}

// findPathsWithIdenticalSizes reads paths from regularFilesCh and, once there
// are more than threshold paths with the same size, writes them to
// pathsToHashCh.
func findPathsWithIdenticalSizes(pathsToHashCh chan<- pathWithSize, regularFilesCh <-chan pathWithSize, threshold int) error {
	allPathsBySize := make(map[int64][]pathWithSize)
	for pathWithSize := range regularFilesCh {
		pathsBySize := append(allPathsBySize[pathWithSize.size], pathWithSize)
		allPathsBySize[pathWithSize.size] = pathsBySize
		if len(pathsBySize) == threshold {
			for _, p := range pathsBySize {
				pathsToHashCh <- p
			}
		} else if len(pathsBySize) > threshold {
			pathsToHashCh <- pathWithSize
		}
	}
	return nil
}

// hashPaths reads paths from pathsToHashCh, computes their hashes, and writes
// them to pathsWithHashCh.
func hashPaths(ctx context.Context, pathsWithHashCh chan<- pathWithHash, pathsToHashCh <-chan pathWithSize) error {
	errGroup, _ := errgroup.WithContext(ctx)
	for pathWithSize := range pathsToHashCh {
		pathWithSize := pathWithSize
		errGroup.Go(func() error {
			pathWithHash, err := pathWithSize.pathWithHash()
			if err != nil {
				return err
			}
			pathsWithHashCh <- pathWithHash
			return nil
		})
	}
	return errGroup.Wait()
}

// pathWithHash hashes p and returns a pathWithHash.
func (p pathWithSize) pathWithHash() (pathWithHash, error) {
	hash, err := p.hash()
	if err != nil {
		return pathWithHash{}, err
	}
	pathWithHash := pathWithHash{
		path: p.path,
	}
	copy(pathWithHash.hash[:], hash)
	return pathWithHash, nil
}

// hash returns p's hash.
func (p pathWithSize) hash() ([]byte, error) {
	if p.size == 0 {
		return sha256SumZero, nil
	}
	file, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	statistics.filesOpened.Add(1)
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, file)
	if err != nil {
		return nil, err
	}
	statistics.bytesHashed.Add(uint64(written))
	return hash.Sum(nil), nil
}

func run() error {
	// Parse command line arguments.
	threshold := pflag.IntP("threshold", "n", 2, "threshold")
	printStatistics := pflag.BoolP("statistics", "s", false, "print statistics")
	pflag.Parse()
	var root string
	switch pflag.NArg() {
	case 0:
		root = "."
	case 1:
		root = pflag.Arg(0)
	default:
		return fmt.Errorf("expected 0 or 1 arguments, got %d", pflag.NArg())
	}

	// Create an errgroup to synchronize goroutines.
	errGroup, ctx := errgroup.WithContext(context.Background())

	// Generate paths with size.
	regularFilesCh := make(chan pathWithSize, channelBufferCapacity)
	errGroup.Go(func() error {
		defer close(regularFilesCh)
		return findRegularFiles(ctx, regularFilesCh, root)
	})

	// Generate paths with size to hash.
	pathsToHashCh := make(chan pathWithSize, channelBufferCapacity)
	errGroup.Go(func() error {
		defer close(pathsToHashCh)
		return findPathsWithIdenticalSizes(pathsToHashCh, regularFilesCh, *threshold)
	})

	// Generate paths with hashes.
	pathsWithHashCh := make(chan pathWithHash, channelBufferCapacity)
	errGroup.Go(func() error {
		defer close(pathsWithHashCh)
		return hashPaths(ctx, pathsWithHashCh, pathsToHashCh)
	})

	// Accumulate paths by hash.
	pathsByHash := make(map[hash][]string)
	errGroup.Go(func() error {
		for pathWithHash := range pathsWithHashCh {
			pathsByHash[pathWithHash.hash] = append(pathsByHash[pathWithHash.hash], pathWithHash.path)
		}
		return nil
	})

	// Wait for all goroutines to finish.
	if err := errGroup.Wait(); err != nil {
		return err
	}

	// Find all duplicates, indexed by hex string of their checksum.
	result := make(map[string][]string, len(pathsByHash))
	for checksum, paths := range pathsByHash {
		if len(paths) >= *threshold {
			key := hex.EncodeToString(checksum[:])
			result[key] = paths
		}
	}

	// Write JSON output.
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}

	// Print statistics.
	if *printStatistics {
		dirEntries := statistics.dirEntries.Load()
		filesOpened := statistics.filesOpened.Load()
		totalBytes := statistics.totalBytes.Load()
		bytesHashed := statistics.bytesHashed.Load()
		fmt.Fprintf(os.Stderr, "dirEntries: %d\n", dirEntries)
		fmt.Fprintf(os.Stderr, "filesOpened: %d (%.1f%%)\n", filesOpened, 100*float64(filesOpened)/float64(dirEntries)+0.05)
		fmt.Fprintf(os.Stderr, "totalBytes: %d\n", totalBytes)
		fmt.Fprintf(os.Stderr, "bytesHashed: %d (%.1f%%)\n", bytesHashed, 100*float64(bytesHashed)/float64(totalBytes)+0.05)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
