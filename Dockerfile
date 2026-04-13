FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y cmake clang pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock* ./
COPY src/ src/

RUN cargo build --release --bin cm

FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates aria2 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/cm /usr/local/bin/cm

ENTRYPOINT ["cm"]
CMD ["daemon", "--config", "/etc/cm/config.toml"]
