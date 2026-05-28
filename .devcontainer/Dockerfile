FROM rust:1.85-bookworm

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Install cargo tools
RUN cargo install --locked cargo-deny@0.18.3 cargo-watch

# Set up workspace
WORKDIR /workspace

# Pre-fetch dependencies (will be overwritten by mount)
COPY Cargo.toml Cargo.lock* ./
COPY crates/ crates/
RUN cargo fetch || true

ENV RUST_LOG=info
