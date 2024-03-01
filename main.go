// find-duplicates finds duplicate files, concurrently.
package main

// FIXME operate on io/fs.FS

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/trace"

	"github.com/spf13/pflag"

	"github.com/twpayne/find-duplicates/internal/dupfind"
)

func run() error {
	numCPU := runtime.NumCPU()

	// Parse command line arguments.
	var (
		keepGoing       = pflag.BoolP("keep-going", "k", false, "keep going after errors")
		threshold       = pflag.IntP("threshold", "n", 2, "threshold")
		output          = pflag.StringP("output", "o", "", "output file")
		printStatistics = pflag.BoolP("statistics", "s", false, "print statistics")
		traceFile       = pflag.String("trace", "", "trace file")
		hasherLimit     = pflag.IntP("hasher-limit", "h", 8*numCPU, "maximum number of hashers")
		walkerLimit     = pflag.IntP("walker-limit", "w", 32*numCPU, "maximum number of walkers")
	)
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

	// Find duplicates.
	options := []dupfind.Option{
		dupfind.WithHasherLimit(*hasherLimit),
		dupfind.WithRoots(roots...),
		dupfind.WithThreshold(*threshold),
		dupfind.WithWalkerLimit(*walkerLimit),
	}
	if *keepGoing {
		option := dupfind.WithErrorHandler(func(err error) error {
			fmt.Fprintln(os.Stderr, err)
			return nil
		})
		options = append(options, option)
	}
	dupFinder := dupfind.NewDupFinder(options...)
	result, err := dupFinder.FindDuplicates()
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
