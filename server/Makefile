.PHONY: build test lint run clean

RELEASE_BINARIES := \
	vane=./cmd/server \
	useradmin=./cmd/useradmin \
	gate=./cmd/gate \
	runtimeadmin=./cmd/runtimeadmin \
	vane-migrate=./cmd/migrate \
	agentfirstretention=./cmd/agentfirstretention \
	vane-research-gateway=./cmd/researchgateway \
	vane-research-prepare=./cmd/researchprepare \
	researchshadow=./cmd/researchshadow \
	researchcutover=./cmd/researchcutover

build:
	@set -eu; for entry in $(RELEASE_BINARIES); do \
		name=$${entry%%=*}; package=$${entry#*=}; \
		GOWORK=off go build -buildvcs=true -o "bin/$$name" "$$package"; \
	done

test:
	GOWORK=off go test -race -coverprofile=coverage.txt ./...

lint:
	golangci-lint run ./...

run:
	GOWORK=off go run ./cmd/server

clean:
	rm -rf bin/ coverage.txt
