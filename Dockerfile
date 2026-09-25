FROM rust:1.94.0-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY server ./server
COPY binaries ./binaries
ARG SOURCE_REVISION=unknown
RUN SOURCE_REVISION="$SOURCE_REVISION" cargo build --locked --release --bin websocket_server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/websocket_server /usr/local/bin/websocket_server
ENV RUST_LOG=info
ENV WS_WALLET_POLL_INTERVAL_MS=30000
USER 1000:1000
EXPOSE 8000
ENTRYPOINT ["websocket_server"]
