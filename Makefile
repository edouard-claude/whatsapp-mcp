# Construit les deux binaires côte à côte : wa-mcp cherche wa-bridge dans son répertoire.
PROFILE ?= debug
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
CARGO_FLAGS := $(if $(filter release,$(PROFILE)),--release,)

.PHONY: build proto check test update-whatsmeow sync-whatsmeow-proto check-whatsmeow

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

# Monte whatsmeow à son dernier commit, puis recopie ses .proto (compilés par prost).
update-whatsmeow:
	cd bridge && go get go.mau.fi/whatsmeow@latest && go mod tidy
	$(MAKE) sync-whatsmeow-proto

# Recopie les .proto de whatsmeow au commit fixé dans bridge/go.mod.
sync-whatsmeow-proto:
	cd bridge && go mod download go.mau.fi/whatsmeow
	src=$$(cd bridge && go list -m -f '{{.Dir}}' go.mau.fi/whatsmeow)/proto && \
	rm -rf proto/whatsmeow && \
	for f in $$(cd $$src && find . -name '*.proto'); do \
		mkdir -p proto/whatsmeow/$$(dirname $$f) && cp $$src/$$f proto/whatsmeow/$$f && chmod 644 proto/whatsmeow/$$f; \
	done && \
	(cd bridge && go list -m -f '{{.Version}}' go.mau.fi/whatsmeow) > proto/whatsmeow/VERSION

# Les .proto recopiés doivent correspondre au whatsmeow du bridge.
check-whatsmeow:
	test "$$(cat proto/whatsmeow/VERSION)" = "$$(cd bridge && go list -m -f '{{.Version}}' go.mau.fi/whatsmeow)"
