# Directory containing the Makefile.
PROJECT_ROOT = $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

export GOBIN ?= $(PROJECT_ROOT)/bin
export PATH := $(GOBIN):$(PATH)

GOVULNCHECK = $(GOBIN)/govulncheck
CHANGIE = $(GOBIN)/changie
BENCH_FLAGS ?= -cpuprofile=cpu.pprof -memprofile=mem.pprof -benchmem

# Directories containing independent Go modules.
MODULE_DIRS = . ./exp ./benchmarks ./zapgrpc/internal/test

# Directories that we want to track coverage for.
COVER_DIRS = . ./exp

.PHONY: all
all: lint test

.PHONY: lint
lint: golangci-lint tidy-lint license-lint

.PHONY: golangci-lint
golangci-lint:
	@$(foreach mod,$(MODULE_DIRS), \
		(cd $(mod) && \
		echo "[lint] golangci-lint: $(mod)" && \
		golangci-lint run --path-prefix $(mod)) &&) true

.PHONY: tidy
tidy:
	@$(foreach dir,$(MODULE_DIRS), \
		(cd $(dir) && go mod tidy) &&) true

.PHONY: tidy-lint
tidy-lint:
	@$(foreach mod,$(MODULE_DIRS), \
		(cd $(mod) && \
		echo "[lint] tidy: $(mod)" && \
		go mod tidy && \
		git diff --exit-code -- go.mod go.sum) &&) true


.PHONY: license-lint
license-lint:
	./checklicense.sh

.PHONY: tools
tools: $(GOVULNCHECK) $(CHANGIE)

$(GOVULNCHECK):
	cd tools && go install golang.org/x/vuln/cmd/govulncheck

$(CHANGIE):
	cd tools && go install github.com/miniscruff/changie

.PHONY: test
test:
	@$(foreach dir,$(MODULE_DIRS),(cd $(dir) && go test -race ./...) &&) true

.PHONY: cover
cover:
	@$(foreach dir,$(COVER_DIRS), ( \
		cd $(dir) && \
		go test -race -coverprofile=cover.out -coverpkg=./... ./... \
		&& go tool cover -html=cover.out -o cover.html) &&) true

.PHONY: bench
BENCH ?= .
bench:
	@$(foreach dir,$(MODULE_DIRS), ( \
		cd $(dir) && \
		go list ./... | xargs -n1 go test -bench=$(BENCH) -run="^$$" $(BENCH_FLAGS) \
	) &&) true

.PHONY: updatereadme
updatereadme:
	rm -f README.md
	cat .readme.tmpl | go run internal/readme/readme.go > README.md

.PHONY: vulncheck
vulncheck: $(GOVULNCHECK)
	$(GOVULNCHECK) ./...

## Changelog management targets:
#
# changelog/all, changelog/zap, changelog/exp
#   Generates combined changelogs for all or specific modules.
#
# change-new/<module>
#   Creates a new changelog entry for the given module.
#
# change-batch/<module> VERSION=[major|minor|patch]
#   Batch unreleased changes for the module into a new release.

.PHONY: changelog/all
changelog/all: changelog/zap changelog/exp

.PHONE: changelog/%
changelog/%: $(CHANGIE)
	$(CHANGIE) merge -j $*

.PHONY: change-new/%
change-new/%: $(CHANGIE)
	$(CHANGIE) new -j $*

.PHONY: change-new
change-new:
	@echo "Usage: make change-new/<module>"
	@echo "  where <module> is one of: zap, exp"

.PHONY: change-batch/%
change-batch/%: $(CHANGIE)
	@VERSION=$${VERSION:?Please set VERSION}; \
	$(CHANGIE) batch -j $* $$VERSION
