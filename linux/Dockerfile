# Official immutable Linux/arm64 manifest (Rust 1.89.0, Debian Bookworm).
FROM docker.io/library/rust:1.89-bookworm@sha256:587f5c8de927f3360b160126b7cd42b2215e2f055dfbe2eb1890a9b056fb5c38
ARG VZ_LINUX_RUST_VERSION=1.89.0
RUN test "$(rustc --version | cut -d' ' -f2)" = "$VZ_LINUX_RUST_VERSION"

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
    python3 \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add aarch64-unknown-linux-musl

WORKDIR /workspace
