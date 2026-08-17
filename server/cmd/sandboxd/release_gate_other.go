//go:build !linux

package main

import (
	"context"
	"io"
)

func runReleaseGate(context.Context, []string, io.Writer, io.Writer) int {
	return 1
}

func runReleaseGateNetNSPreflight([]string, io.Writer, io.Writer) int { return 1 }

func runReleaseGateReap([]string, io.Writer) int { return 1 }
