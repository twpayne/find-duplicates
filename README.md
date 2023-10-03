# `find-duplicates`

`find-duplicates` finds duplicate files quickly based on the
[xxHashes](https://xxhash.com/) of their contents.

## Install

```console
$ go install github.com/twpayne/find-duplicates@latest
```

## Usage

```
find-duplicates [options] [path]
```

`path` is a directory to walk recursively. If `path` is not specified then the
current directory is walked.

The output is a JSON object with properties for each observed xxHash and values
arrays of filenames with contents with that xxHash.

Options are:

`--threshold=<int>` the minimum number of files with the same content to be
considered duplicates. The default is `2`.

`--statistics` print statistics to stderr.

## License

MIT