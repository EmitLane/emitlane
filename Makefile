.PHONY: fmt vet lint test test-race test-integration vuln build check soak-build soak-start soak-status soak-logs soak-stop soak-report

SOAK_BIN := .emitlane/bin/emitlane-soak
PROFILE ?= quick

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

soak-build:
	@mkdir -p .emitlane/bin
	@go build -o $(SOAK_BIN) ./cmd/emitlane-soak

soak-start: soak-build
	@$(SOAK_BIN) start --profile "$(PROFILE)" $(if $(DURATION),--duration "$(DURATION)") $(if $(RECOVERY_TIMEOUT),--recovery-timeout "$(RECOVERY_TIMEOUT)") $(if $(SEED),--seed "$(SEED)") $(if $(RELAYS),--relays "$(RELAYS)") $(if $(STREAMS),--streams "$(STREAMS)") $(if $(RATE),--rate "$(RATE)") $(if $(ALLOW_DIRTY),--allow-dirty)

soak-status:
	@test -x $(SOAK_BIN) || $(MAKE) --no-print-directory soak-build
	@$(SOAK_BIN) status

soak-logs:
	@test -x $(SOAK_BIN) || $(MAKE) --no-print-directory soak-build
	@$(SOAK_BIN) logs

soak-stop:
	@test -x $(SOAK_BIN) || $(MAKE) --no-print-directory soak-build
	@$(SOAK_BIN) stop

soak-report:
	@test -x $(SOAK_BIN) || $(MAKE) --no-print-directory soak-build
	@$(SOAK_BIN) report
