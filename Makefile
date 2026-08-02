.PHONY: build test lint run clean

build:
	go build -o bin/vane ./cmd/server
	go build -o bin/vane-migrate ./cmd/migrate
	go build -o bin/vane-research-gateway ./cmd/researchgateway

test:
	go test -race -coverprofile=coverage.txt ./...

lint:
	golangci-lint run ./...

run:
	go run ./cmd/server

clean:
	rm -rf bin/ coverage.txt
