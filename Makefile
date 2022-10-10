.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/ahamidi/conduit-connector-pulsar.version=${VERSION}'" -o conduit-connector-pulsar cmd/connector/main.go

test:
	docker-compose -f test/docker-compose.yml up --quiet-pull -d
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker-compose -f test/docker-compose.yml down; \
		exit $$ret

lint:
	golangci-lint run