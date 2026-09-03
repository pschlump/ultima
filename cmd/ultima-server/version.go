// Build-stamp placeholders, overwritten by -ldflags -X via
// bin/gen-build-stamp.sh (design doc §8).
package main

var (
	GitCommit     = "unknown"
	Version       = "dev"
	BuildDate     = "unknown"
	GitBranchName = "unknown"
	BuildTarget   = "unknown"
)
