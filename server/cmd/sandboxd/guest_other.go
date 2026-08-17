//go:build !linux

package main

import (
	"io"
)

func runGuest([]string, io.Writer, io.Writer) int {
	return 1
}
