FROM rust:1.85-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    bc \
    bison \
    build-essential \
    ca-certificates \
    cpio \
    curl \
    file \
    flex \
    libelf-dev \
    libssl-dev \
    musl \
    musl-dev \
    musl-tools \
    pkg-config \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add aarch64-unknown-linux-musl

WORKDIR /workspace
