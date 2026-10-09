.PHONY: build-bridge test test-bridge test-mcp install-bridge

build-bridge:
	cd whatsapp-bridge && go build -tags "fts5" -o whatsapp-bridge .

install-bridge:
	cd whatsapp-bridge && go build -tags "fts5" -o $(HOME)/.local/share/whatsapp-mcp/bin/whatsapp-bridge .

test-bridge:
	cd whatsapp-bridge && go test -tags "fts5" ./...

test-mcp:
	cd whatsapp-mcp-server && /home/jeanmartina/.local/share/whatsapp-mcp/venv/bin/pytest

test: test-bridge test-mcp
