FROM rust:1.91-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

ENV FEATURES=nats,database
ENV RUST_BACKTRACE=1
ENV RUNTIME_SNAPSHOT_PATH=/build/snapshot.bin

RUN touch $RUNTIME_SNAPSHOT_PATH

COPY --from=planner /build/recipe.json recipe.json

RUN --mount=type=cache,target=$CARGO_HOME/git \
    --mount=type=cache,target=$CARGO_HOME/registry \
    --mount=type=cache,target=/build/target \
    cargo chef cook --release --features=$FEATURES --recipe-path recipe.json

COPY . .

RUN touch $RUNTIME_SNAPSHOT_PATH

RUN --mount=type=cache,target=$CARGO_HOME/git \
    --mount=type=cache,target=$CARGO_HOME/registry \
    --mount=type=cache,target=/build/target \
    cargo run --release --features=$FEATURES --bin snapshot && \
    cargo build --release --features=$FEATURES && \
    cp /build/target/release/task-executor /build/output

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/output /usr/local/bin/task-executor
COPY --from=builder /build/snapshot.bin /build/snapshot.bin

ENTRYPOINT ["/usr/local/bin/task-executor"]
