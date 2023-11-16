# `find-duplicates`

`find-duplicates` finds duplicate files quickly based on the
[xxHashes](https://xxhash.com/) of their contents.

## Installation

```console
$ go install github.com/twpayne/find-duplicates@latest
```

## Example

```console
$ find-duplicates
{
  "cdb8979062cbdf9c169563ccc54704f0": [
    ".git/refs/remotes/origin/main",
    ".git/refs/heads/main",
    ".git/ORIG_HEAD"
  ]
}
```

## Usage

```
find-duplicates [options] [paths...]
```

`paths` are directories to walk recursively. If no `paths` are given then the
current directory is walked.

The output is a JSON object with properties for each observed xxHash and values
arrays of filenames with contents with that xxHash.

Options are:

`--keep-going` keep going after errors.

`--threshold=<int>` sets the minimum number of files with the same content to be
considered duplicates. The default is 2.

`--statistics` prints statistics to stderr.

## How does `find-duplicates` work?

`find-duplicates` aims to be as fast as possible by doing as little work as
possible, using each CPU core efficiently, and using all the CPU cores on your
machine.

It consists of multiple components:

1. Firstly, it walks the the filesystem concurrently, spawning one goroutine per
   subdirectory.
2. Secondly, with the observation that files can only be duplicates if they are
   the same size, it only reads file contents once it has found at more than one
   file with the same size. This significantly reduces both the number of
   syscalls and the amount of data read.
3. Thirdly, files contents are hashed with a fast, non-cryptographic hash.

All components run concurrently.

## Media

* ["Finding duplicate files unbelievably fast: a small CLI project using Go's concurrency"](https://www.youtube.com/watch?v=wJ7-Y55Esio) talk from [Zürich Gophers](https://www.meetup.com/zurich-gophers/).

## License

MIT