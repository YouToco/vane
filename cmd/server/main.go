package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("vane server starting...")
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fmt.Println("vane v0.0.1")
	return nil
}
