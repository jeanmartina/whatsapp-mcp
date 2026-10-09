.PHONY: build test clean install

BINARY_NAME=whatsapp-bridge
BUILD_TAGS=fts5

build:
	go build -tags "$(BUILD_TAGS)" -o $(BINARY_NAME) .

test:
	go test -tags "$(BUILD_TAGS)" -v ./...

install:
	go build -tags "$(BUILD_TAGS)" -o $(HOME)/.local/share/whatsapp-mcp/bin/$(BINARY_NAME) .

clean:
	rm -f $(BINARY_NAME)
