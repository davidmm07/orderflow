# syntax=docker/dockerfile:1

# Build stage. Compiles both release binaries with their Kafka adapters.
FROM rust:1.98.1-slim-trixie AS build

# rdkafka-sys compiles librdkafka from source and needs a C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
        -p orderflow-server -p orderflow-pricefeed \
        --features orderflow-server/kafka,orderflow-pricefeed/kafka \
    && cp target/release/orderflow target/release/pricefeed /usr/local/bin/

# Price feed image: docker build --target pricefeed .
# Distroless: no shell, no package manager, non-root user. Its CA bundle is
# what the TLS clients trust when calling exchanges and RPC nodes.
FROM gcr.io/distroless/cc-debian13:nonroot AS pricefeed

COPY --from=build /usr/local/bin/pricefeed /usr/local/bin/pricefeed
COPY config/marketdata.json /etc/orderflow/marketdata.json

ENV PRICEFEED_BIND_ADDR=0.0.0.0:8090 \
    PRICEFEED_FEEDS_FILE=/etc/orderflow/marketdata.json \
    PRICEFEED_LOG_FORMAT=json

EXPOSE 8090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/pricefeed"]

# Exchange image, the default target: docker build .
FROM gcr.io/distroless/cc-debian13:nonroot AS orderflow

COPY --from=build /usr/local/bin/orderflow /usr/local/bin/orderflow
COPY config/instruments.json /etc/orderflow/instruments.json

ENV ORDERFLOW_BIND_ADDR=0.0.0.0:8080 \
    ORDERFLOW_INSTRUMENTS_FILE=/etc/orderflow/instruments.json \
    ORDERFLOW_LOG_FORMAT=json

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/orderflow"]
