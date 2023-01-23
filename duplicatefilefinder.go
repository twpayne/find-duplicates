package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/sourcegraph/conc/pool"
	"golang.org/x/exp/maps"
)

// A duplicateFileFinder finds duplicate files concurrently.
type duplicateFileFinder struct {
	sync.Mutex
	filenamesSetBySHA256Hex map[string]map[string]struct{}
	pool                    *pool.ErrorPool
	threshold               int
}

// newDuplicateFileFinderOptions are options to newDuplicateFileFinder.
type newDuplicateFileFinderOptions struct {
	maxGoroutines int
	threshold     int
}

// newDuplicateFileFinder returns a new duplicateFileFinder.
func newDuplicateFileFinder(options newDuplicateFileFinderOptions) *duplicateFileFinder {
	return &duplicateFileFinder{
		filenamesSetBySHA256Hex: make(map[string]map[string]struct{}),
		pool:                    pool.New().WithErrors().WithMaxGoroutines(options.maxGoroutines),
		threshold:               options.threshold,
	}
}

// scanDir walks dirname, adding tasks to scan all regular files.
func (f *duplicateFileFinder) scanDir(dirname string) error {
	return fs.WalkDir(os.DirFS(dirname), ".", func(path string, dirEntry fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case dirEntry.Type() == 0:
			f.pool.Go(f.scanFileFunc(filepath.Join(dirname, path)))
			return nil
		default:
			return nil
		}
	})
}

// scanFile adds a task to scan filename.
func (f *duplicateFileFinder) scanFile(filename string) {
	f.pool.Go(f.scanFileFunc(filename))
}

// scanFileFunc returns a function that scans filename.
func (f *duplicateFileFinder) scanFileFunc(filename string) func() error {
	return func() error {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			return err
		}
		sha256Hex := hex.EncodeToString(hash.Sum(nil))
		f.Lock()
		defer f.Unlock()
		filenames, ok := f.filenamesSetBySHA256Hex[sha256Hex]
		if !ok {
			filenames = make(map[string]struct{})
			f.filenamesSetBySHA256Hex[sha256Hex] = filenames
		}
		filenames[filename] = struct{}{}
		return nil
	}
}

// collect waits for all of f's tasks to complete and returns a map of hex-encoded
// SHA256 hashes to filenames with contents with that SHA256 hash.
func (f *duplicateFileFinder) collect() (map[string][]string, error) {
	if err := f.pool.Wait(); err != nil {
		return nil, err
	}

	filenamesBySHA256Hex := make(map[string][]string)
	for sha256Hex, set := range f.filenamesSetBySHA256Hex {
		if len(set) < f.threshold {
			continue
		}
		filenames := maps.Keys(set)
		sort.Strings(filenames)
		filenamesBySHA256Hex[sha256Hex] = filenames
	}
	return filenamesBySHA256Hex, nil
}
