//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "godfs-fuse is supported on Linux only")
	os.Exit(2)
}

