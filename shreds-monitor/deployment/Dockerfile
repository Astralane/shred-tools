FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev cmake protobuf-compiler pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/shreds-monitor /usr/local/bin/shreds-monitor
ENTRYPOINT ["shreds-monitor"]
