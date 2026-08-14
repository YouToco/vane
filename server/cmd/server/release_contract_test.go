package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestMainPrintsMachineReleaseContractWithoutStartingServer(t *testing.T) {
	originalArgs, originalStdout := os.Args, os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Args, os.Stdout = []string{"vane", "-print-release-contract"}, writer
	t.Cleanup(func() {
		os.Args, os.Stdout = originalArgs, originalStdout
		reader.Close()
		writer.Close()
	})

	main()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(payload)) != serverReleaseContractV2 {
		t.Fatalf("release contract output=%q", payload)
	}
}
