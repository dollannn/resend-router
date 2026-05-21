# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1
ARG DEBIAN_VERSION=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS chef
WORKDIR /app
RUN cargo install cargo-chef --locked

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --locked --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/resend-router /usr/local/bin/resend-router

FROM debian:${DEBIAN_VERSION}-slim AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN useradd \
    --uid 10001 \
    --gid root \
    --no-create-home \
    --home-dir /nonexistent \
    --shell /usr/sbin/nologin \
    appuser
COPY --from=builder /usr/local/bin/resend-router /usr/local/bin/resend-router

USER appuser
EXPOSE 3000
ENV RUST_LOG=info,resend_router=debug,tower_http=info

CMD ["/usr/local/bin/resend-router"]
