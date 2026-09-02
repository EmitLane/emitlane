.PHONY: fmt vet lint test test-race test-integration vuln build check

fmt:
	gofmt -w .

vet:
	go vet ./...

lint:
	staticcheck ./...

test:
	go test -count=1 ./...

test-race:
	go test -race -count=1 ./...

test-integration:
	go test -tags=integration -count=1 -timeout=20m ./...

vuln:
	govulncheck ./...

build:
	go build -o bin/emitlane ./cmd/emitlane

check: fmt vet lint test vuln build
	@test -z "$$(gofmt -l .)"
