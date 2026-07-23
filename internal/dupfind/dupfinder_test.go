package dupfind_test

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/twpayne/go-vfs/v5/vfst"
	"github.com/zeebo/xxh3"

	"github.com/twpayne/find-duplicates/internal/dupfind"
)

func TestDupFinder(t *testing.T) {
	for _, tc := range []struct {
		name               string
		root               any
		options            []dupfind.Option
		expected           map[string][]string
		expectedStatistics *dupfind.Statistics
	}{
		{
			name:     "empty",
			options:  []dupfind.Option{dupfind.WithHashFunc(sha256.New)},
			expected: map[string][]string{},
		},
		{
			name: "no_duplicates",
			root: map[string]any{
				"alpha": "a",
			},
			options:  []dupfind.Option{dupfind.WithHashFunc(sha256.New)},
			expected: map[string][]string{},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:  1,
				Files:       1,
				TotalBytes:  1,
				UniqueSizes: 1,
			},
		},
		{
			name: "one_duplicate_unique_sizes",
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
				"gamma": "aa",
			},
			options: []dupfind.Option{dupfind.WithHashFunc(sha256.New)},
			expected: map[string][]string{
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         3,
				Files:              3,
				FilesOpened:        2,
				FilesOpenedPercent: 100 * 2. / 3,
				TotalBytes:         4,
				BytesHashed:        2,
				BytesHashedPercent: 50,
				UniqueSizes:        2,
			},
		},
		{
			name: "one_duplicate_repeated_sizes",
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
				"gamma": "b",
			},
			options: []dupfind.Option{dupfind.WithHashFunc(sha256.New)},
			expected: map[string][]string{
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         3,
				Files:              3,
				FilesOpened:        3,
				FilesOpenedPercent: 100,
				TotalBytes:         3,
				BytesHashed:        3,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "one_duplicate_recursive",
			root: map[string]any{
				"alpha": "a",
				"dir": map[string]any{
					"beta": "a",
				},
			},
			options: []dupfind.Option{dupfind.WithHashFunc(sha256.New)},
			expected: map[string][]string{
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"dir/beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         3,
				Files:              2,
				FilesOpened:        2,
				FilesOpenedPercent: 100,
				TotalBytes:         2,
				BytesHashed:        2,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "two_duplicates",
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
				"gamma": "b",
				"delta": "b",
			},
			options: []dupfind.Option{dupfind.WithHashFunc(sha256.New)},
			expected: map[string][]string{
				"3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d": {
					"delta",
					"gamma",
				},
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         4,
				Files:              4,
				FilesOpened:        4,
				FilesOpenedPercent: 100,
				TotalBytes:         4,
				BytesHashed:        4,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "exclude_pattern",
			options: []dupfind.Option{
				dupfind.WithHashFunc(sha256.New),
				dupfind.WithIncludeFunc(func(p string) bool {
					_, basename := path.Split(p)
					return basename != "delta"
				}),
			},
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
				"gamma": "b",
				"delta": "b",
			},
			expected: map[string][]string{
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         4,
				Files:              4,
				FilesOpened:        3,
				FilesOpenedPercent: 75,
				TotalBytes:         3,
				BytesHashed:        3,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "exclude_pattern_dir",
			options: []dupfind.Option{
				dupfind.WithHashFunc(sha256.New),
				dupfind.WithIncludeFunc(func(p string) bool {
					_, basename := path.Split(p)
					return basename != "x"
				}),
			},
			root: map[string]any{
				"alpha":   "a",
				"beta":    "a",
				"x/alpha": "a",
				"x/beta":  "b",
				"y/alpha": "a",
			},
			expected: map[string][]string{
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"beta",
					"y/alpha",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         5,
				Files:              3,
				FilesOpened:        3,
				FilesOpenedPercent: 100,
				TotalBytes:         3,
				BytesHashed:        3,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "sha256",
			options: []dupfind.Option{
				dupfind.WithHashFunc(sha256.New),
			},
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
			},
			expected: map[string][]string{
				"ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         2,
				Files:              2,
				FilesOpened:        2,
				FilesOpenedPercent: 100,
				TotalBytes:         2,
				BytesHashed:        2,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "sha512",
			options: []dupfind.Option{
				dupfind.WithHashFunc(sha512.New),
			},
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
			},
			expected: map[string][]string{
				"1f40fc92da241694750979ee6cf582f2d5d7d28e18335de05abc54d0560e0f5302860c652bf08d560252aa5e74210546f369fbbbce8c12cfc7957b2652fe9a75": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         2,
				Files:              2,
				FilesOpened:        2,
				FilesOpenedPercent: 100,
				TotalBytes:         2,
				BytesHashed:        2,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
		{
			name: "xxhash",
			options: []dupfind.Option{
				dupfind.WithHashFunc(func() hash.Hash { return xxh3.New() }),
			},
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
			},
			expected: map[string][]string{
				"e6c632b61e964e1f": {
					"alpha",
					"beta",
				},
			},
			expectedStatistics: &dupfind.Statistics{
				DirEntries:         2,
				Files:              2,
				FilesOpened:        2,
				FilesOpenedPercent: 100,
				TotalBytes:         2,
				BytesHashed:        2,
				BytesHashedPercent: 100,
				UniqueSizes:        1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			fs, cleanup, err := vfst.NewTestFS(tc.root)
			assert.NoError(t, err)
			defer cleanup()

			options := slices.Clone(tc.options)
			options = append(options, dupfind.WithRoots(fs.TempDir()))
			dupFinder := dupfind.NewDupFinder(options...)
			actual, err := dupFinder.FindDuplicates(ctx)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, trimValuePrefixes(actual, fs.TempDir()+"/"))

			if tc.expectedStatistics != nil {
				assert.Equal(t, tc.expectedStatistics, dupFinder.Statistics())
			}
		})
	}
}

func trimValuePrefixes(m map[string][]string, prefix string) map[string][]string {
	result := make(map[string][]string, len(m))
	for key, value := range m {
		result[key] = trimPrefixes(value, prefix)
	}
	return result
}

func trimPrefixes(ss []string, prefix string) []string {
	result := make([]string, 0, len(ss))
	for _, s := range ss {
		result = append(result, strings.TrimPrefix(s, prefix))
	}
	return result
}
