// find-duplicates finds duplicate files, concurrently.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"runtime/trace"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/spf13/pflag"
	"github.com/zeebo/xxh3"

	"github.com/twpayne/find-duplicates/internal/dupfind"
)

var hashFuncs = map[string]func() hash.Hash{
	"sha256": sha256.New,
	"sha512": sha512.New,
	"xxhash": func() hash.Hash { return xxh3.New() },
}

func run() error {
	ctx := context.Background()

	// Parse command line arguments.
	excludePatterns := pflag.StringSliceP("exclude", "x", nil, "exclude patterns")
	hash := pflag.StringP("hash", "h", "xxhash", "hash to use (sha256, sha512, or xxhash)")
	keepGoing := pflag.BoolP("keep-going", "k", false, "keep going after errors")
	threshold := pflag.IntP("threshold", "n", 2, "threshold")
	output := pflag.StringP("output", "o", "", "output file")
	printStatistics := pflag.BoolP("statistics", "s", false, "print statistics")
	traceFile := pflag.String("trace", "", "trace file")
	pflag.Parse()
	var roots []string
	if pflag.NArg() == 0 {
		roots = []string{"."}
	} else {
		roots = pflag.Args()
	}

	// Create a trace file, if requested.
	if *traceFile != "" {
		traceFile, err := os.Create(*traceFile)
		if err != nil {
			return err
		}
		defer traceFile.Close()
		if err := trace.Start(traceFile); err != nil {
			return err
		}
		defer trace.Stop()
	}

	for _, excludePattern := range *excludePatterns {
		if !doublestar.ValidatePattern(excludePattern) {
			return fmt.Errorf("%s: invalid pattern", excludePattern)
		}
	}

	// Find duplicates.
	hashFunc, ok := hashFuncs[strings.ToLower(*hash)]
	if !ok {
		return fmt.Errorf("%s: invalid hash", *hash)
	}
	options := []dupfind.Option{
		dupfind.WithHashFunc(hashFunc),
		dupfind.WithIncludeFunc(func(path string) bool {
			for _, excludePattern := range *excludePatterns {
				if doublestar.MatchUnvalidated(excludePattern, path) {
					return false
				}
			}
			return true
		}),
		dupfind.WithThreshold(*threshold),
		dupfind.WithRoots(roots...),
	}
	if *keepGoing {
		option := dupfind.WithErrorHandler(func(err error) error {
			fmt.Fprintln(os.Stderr, err)
			return nil
		})
		options = append(options, option)
	}
	dupFinder := dupfind.NewDupFinder(options...)
	result, err := dupFinder.FindDuplicates(ctx)
	if err != nil {
		return err
	}

	// Write output file.
	var outputFile *os.File
	if *output == "" || *output == "-" {
		outputFile = os.Stdout
	} else {
		file, err := os.Create(*output)
		if err != nil {
			return err
		}
		defer file.Close()
		outputFile = file
	}
	encoder := json.NewEncoder(outputFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return err
	}

	// Print statistics.
	if *printStatistics {
		encoder := json.NewEncoder(os.Stderr)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(dupFinder.Statistics()); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
