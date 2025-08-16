GITCOMMIT := $(shell git rev-parse HEAD)
GITDATE := $(shell git show -s --format='%ct')

LDFLAGSSTRING +=-X main.GitCommit=$(GITCOMMIT)
LDFLAGSSTRING +=-X main.GitDate=$(GITDATE)
LDFLAGS := -ldflags "$(LDFLAGSSTRING)"

multichain-sync-account:
	env GO111MODULE=on go build -v $(LDFLAGS) ./cmd/multichain-sync-account

clean:
	rm multichain-syncs6-account

test:
	go test -v ./...

protogo:
	sh ./sh/go_compile.sh

lint:
	golangci-lint run ./...

.PHONY: \
	multichain-syncs6-account \
	clean \
	test \
	protogo \
	lint