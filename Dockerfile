# Image du serveur MCP : `docker run -i --rm -v wa-data:/data ghcr.io/…` sur stdio.
# Construit depuis les sources : wa-mcp (Rust) et wa-bridge (Go), côte à côte.

FROM golang:1.27-bookworm AS bridge
WORKDIR /src/bridge
COPY bridge/go.mod bridge/go.sum ./
RUN go mod download
COPY bridge/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/wa-bridge .

FROM rust:1-bookworm AS server
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock clippy.toml ./
COPY crates/ crates/
COPY proto/ proto/
RUN cargo build --release --locked --bin wa-mcp && cp target/release/wa-mcp /out-wa-mcp

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates tzdata && rm -rf /var/lib/apt/lists/*
COPY --from=server /out-wa-mcp /usr/local/bin/wa-mcp
COPY --from=bridge /out/wa-bridge /usr/local/bin/wa-bridge
ENV WA_DATA_DIR=/data
VOLUME ["/data"]
# Sans compte appairé, le serveur démarre et répond quand même (appairage par
# l'outil `session_pair`, ou `docker run -it … wa-mcp pair <alias> --phone <n>`).
ENTRYPOINT ["wa-mcp"]
CMD ["serve"]
