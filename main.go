// find-duplicates finds duplicate files, concurrently.
package main

// FIXME handle multiple roots (arguments)
// FIXME de-duplicate filenames in different roots

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime/pprof"
	"runtime/trace"

	"github.com/spf13/pflag"
	"github.com/twpayne/find-duplicates/internal/find"
)

func run() error {
	// Parse command line arguments.
	cpuProfile := pflag.String("cpu-profile", "", "write CPU profile")
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

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			return err
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
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

	dupFinder := &find.Finder{
		Roots:              roots,
		DuplicateThreshold: *threshold,
		KeepGoing:          *keepGoing,
	}

	result, err := dupFinder.FindDuplicates()
	if err != nil {
		return err
	}

	// Write JSON outputFile.
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
		if err := find.Stats.Print(); err != nil {
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
