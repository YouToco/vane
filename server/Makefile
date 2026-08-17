.PHONY: build test lint run clean

RELEASE_BINARIES := \
	vane=./cmd/server \
	vane-research-gateway=./cmd/researchgateway \
	vane-migrate=./cmd/migrate \
	gate=./cmd/gate \
	agentfirstretention=./cmd/agentfirstretention \
	sandboxd=./cmd/sandboxd

build:
	@set -eu; for entry in $(RELEASE_BINARIES); do \
		name=$${entry%%=*}; package=$${entry#*=}; \
		GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
			go build -buildvcs=true -trimpath -o "bin/$$name" "$$package"; \
	done

test:
	GOWORK=off go test -race -coverprofile=coverage.txt ./...

lint:
	golangci-lint run ./...

run:
	GOWORK=off go run ./cmd/server

clean:
	rm -rf bin/ coverage.txt
