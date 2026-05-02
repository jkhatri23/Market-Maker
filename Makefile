.PHONY: build run test lint tidy clean

BIN := bin/bot
PKG := ./...
CONFIG ?= config.yaml

build:
	go build -o $(BIN) ./cmd/bot

run: build
	./$(BIN) --config $(CONFIG)

test:
	go test -race -count=1 $(PKG)

lint:
	go vet $(PKG)
	gofmt -l . | tee /dev/stderr | (! read)

tidy:
	go mod tidy

clean:
	rm -rf bin/
