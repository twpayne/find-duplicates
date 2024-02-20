package dupfind_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/twpayne/go-vfs/v4/vfst"

	"github.com/twpayne/find-duplicates/internal/dupfind"
)

func TestDupFinder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		root     any
		options  []dupfind.Option
		expected map[string][]string
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
		},
		{
			name: "one_duplicate",
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
