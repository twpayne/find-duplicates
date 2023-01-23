package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/pflag"
)

func run() error {
	maxGoroutines := pflag.IntP("parallism", "j", 4*runtime.GOMAXPROCS(0), "paralellism")
	threshold := pflag.IntP("threshold", "n", 2, "threshold")
	pflag.Parse()

	duplicateFileFinder := newDuplicateFileFinder(newDuplicateFileFinderOptions{
		maxGoroutines: *maxGoroutines,
		threshold:     *threshold,
	})

	if pflag.NArg() == 0 {
		if err := duplicateFileFinder.scanDir("."); err != nil {
			return err
		}
	} else {
		for _, arg := range pflag.Args() {
			switch fileInfo, err := os.Stat(arg); {
			case err != nil:
				return err
			case fileInfo.Mode().Type() == 0:
				duplicateFileFinder.scanFile(arg)
			case fileInfo.IsDir():
				if err := duplicateFileFinder.scanDir(arg); err != nil {
					return err
				}
			}
		}
	}

	filenamesBySHA256Hex, err := duplicateFileFinder.collect()
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(filenamesBySHA256Hex); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
