# syntax=docker/dockerfile:1

# Build stage. Compiles the release binary with the Kafka adapter enabled.
FROM rust:1.98.1-slim-trixie AS build

# rdkafka-sys compiles librdkafka from source and needs a C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p orderflow-server --features kafka \
    && cp target/release/orderflow /usr/local/bin/orderflow

# Runtime stage. Distroless: no shell, no package manager, non-root user.
FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=build /usr/local/bin/orderflow /usr/local/bin/orderflow
COPY config/instruments.json /etc/orderflow/instruments.json

ENV ORDERFLOW_BIND_ADDR=0.0.0.0:8080 \
    ORDERFLOW_INSTRUMENTS_FILE=/etc/orderflow/instruments.json \
    ORDERFLOW_LOG_FORMAT=json

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/orderflow"]
