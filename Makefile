# golars Makefile: common developer tasks.
#
# Usage:
#   make               # default: vet + test
#   make help          # list every target with its description
#   make build         # build every binary under cmd/ to ./bin
#   make install       # go install every binary to $GOBIN
#   make test          # run unit tests across every package
#   make test-race     # race-detector on hot packages
#   make test-simd     # GOEXPERIMENT=simd test run
#   make test-all      # test + test-simd + test-race (the release gate)
#   make bench         # run the polars-compare bench suite
#   make lint          # go vet + staticcheck
#   make fmt           # gofmt + goimports
#   make examples      # compile every ./examples/*
#   make clean         # drop ./bin and pprof files
#   make verify        # rebuild the Lean proofs and regenerate testdata/verified

GO       ?= go
BIN      ?= ./bin
PKGS      = ./...
TESTFLAGS = -count=1
CMDS     := $(notdir $(wildcard ./cmd/*))
LDFLAGS  ?= -s -w
SIMD_ENV ?= GOEXPERIMENT=simd

.PHONY: all help build build-simd install test test-race test-noasm test-simd test-all bench vet lint fmt examples docs-site docker tidy clean check verify

all: check

help: ## Print this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*## */:/' | awk -F: '{printf "  %-14s %s\n", $$1, $$2}'

build: ## Build every binary under cmd/ into ./bin
	@mkdir -p $(BIN)
	@for cmd in $(CMDS); do \
		echo "  build   $$cmd"; \
		$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/$$cmd ./cmd/$$cmd; \
	done

build-simd: ## Build every binary with GOEXPERIMENT=simd (amd64 only)
	@mkdir -p $(BIN)
	@for cmd in $(CMDS); do \
		echo "  build-simd  $$cmd"; \
		$(SIMD_ENV) $(GO) build -ldflags "$(LDFLAGS)" -o $(BIN)/$$cmd-simd ./cmd/$$cmd; \
	done

install: ## go install every command to $GOBIN
	@for cmd in $(CMDS); do \
		echo "  install $$cmd"; \
		$(GO) install -ldflags "$(LDFLAGS)" ./cmd/$$cmd; \
	done

test: ## Run unit tests across every package
	$(GO) test $(TESTFLAGS) $(PKGS)

test-race: ## Race detector on the hot packages
	$(GO) test -race $(TESTFLAGS) ./compute/ ./eval/ ./series/ ./dataframe/ ./lazy/ ./sql/ ./cmd/golars-mcp/

test-noasm: ## Scalar-fallback build path
	$(GO) test -tags noasm $(TESTFLAGS) ./compute/

test-simd: ## SIMD build path
	$(SIMD_ENV) $(GO) test $(TESTFLAGS) $(PKGS)

test-all: test test-simd test-race ## Full release gate

bench: build build-simd ## Run the polars-compare bench harness
	cd bench/polars-compare && uv run python compare.py

vet: ## go vet
	$(GO) vet $(PKGS)

lint: vet ## vet + staticcheck
	@command -v staticcheck >/dev/null 2>&1 || $(GO) install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck $(PKGS)

fmt: ## gofmt + goimports
	@command -v goimports >/dev/null 2>&1 || $(GO) install golang.org/x/tools/cmd/goimports@latest
	gofmt -s -w .
	goimports -w .

examples: ## Compile every ./examples/* (each is its own main package)
	@for dir in ./examples/*/; do \
		if [ -f $$dir/main.go ]; then \
			echo "  build   $$dir"; \
			$(GO) build -o /dev/null $$dir; \
		fi \
	done

docs-site: ## Build the Fumadocs website into docs-site/out
	@cd docs-site && bun install --frozen-lockfile && bun run build

docker: ## Build the Docker image (tag: golars:dev)
	docker build -t golars:dev .

tidy: ## go mod tidy
	$(GO) mod tidy

clean: ## Remove ./bin and pprof files
	rm -rf $(BIN) *.prof *.pprof

verify: ## Rebuild the Lean proofs (verify/) and regenerate testdata/verified
	sh verify/verify.sh
	git diff --stat -- testdata/verified

check: vet test ## Quick gate: vet + test
