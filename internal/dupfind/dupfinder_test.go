package dupfind_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/twpayne/go-vfs/v5/vfst"

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
			expected: map[string][]string{},
		},
		{
			name: "no_duplicates",
			root: map[string]any{
				"alpha": "a",
			},
			expected: map[string][]string{},
			expectedStatistics: &dupfind.Statistics{
				DirEntries: 1,
				Files:      1,
				TotalBytes: 1,
			},
		},
		{
			name: "one_duplicate_unique_sizes",
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
				"gamma": "aa",
			},
			expected: map[string][]string{
				"a96faf705af16834e6c632b61e964e1f": {
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
			},
		},
		{
			name: "one_duplicate_repeated_sizes",
			root: map[string]any{
				"alpha": "a",
				"beta":  "a",
				"gamma": "b",
			},
			expected: map[string][]string{
				"a96faf705af16834e6c632b61e964e1f": {
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
			expected: map[string][]string{
				"a96faf705af16834e6c632b61e964e1f": {
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
			expected: map[string][]string{
				"4b2212e31ac97fd4575a0b1c44d8843f": {
					"delta",
					"gamma",
				},
				"a96faf705af16834e6c632b61e964e1f": {
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
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, cleanup, err := vfst.NewTestFS(tc.root)
			assert.NoError(t, err)
			defer cleanup()

			options := slices.Clone(tc.options)
			options = append(options, dupfind.WithRoots(fs.TempDir()))
			dupFinder := dupfind.NewDupFinder(options...)
			actual, err := dupFinder.FindDuplicates()
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
