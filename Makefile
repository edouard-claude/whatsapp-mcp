# Construit les deux binaires côte à côte : wa-mcp cherche wa-bridge dans son répertoire.
PROFILE ?= debug
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
CARGO_FLAGS := $(if $(filter release,$(PROFILE)),--release,)

.PHONY: build proto check test

build:
	cargo build $(CARGO_FLAGS)
	cd bridge && CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=$(VERSION)" -o ../target/$(PROFILE)/wa-bridge .

# Régénère le code Go du contrat (le code Rust est généré par build.rs).
proto:
	protoc -I proto --go_out=bridge/pb --go_opt=paths=source_relative proto/bridge.proto

check:
	cargo fmt --all --check
	cargo clippy --all-targets -- -D warnings
	cd bridge && go vet ./...

test:
	cargo test
