FROM rust:1.83-slim-bookworm as builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benchmark ./benchmark
COPY benches ./benches

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ssd-kv /usr/local/bin/

RUN mkdir -p /data

EXPOSE 7777

CMD ["ssd-kv", "--data-dir", "/data", "--bind", "0.0.0.0:7777"]
