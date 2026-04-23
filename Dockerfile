FROM rust:1.93-slim-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/ssd-kv /usr/local/bin/ssd-kv

RUN mkdir -p /data

EXPOSE 7777
ENTRYPOINT ["ssd-kv"]
CMD ["--data-dir", "/data", "--bind", "0.0.0.0:7777"]
