# ============================================================
# Stage 1: Build Rust binary
# ============================================================
FROM rust:1.94-bookworm AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./

# Copy crate manifests
COPY crates/ci_runner/Cargo.toml ./crates/ci_runner/

# Create dummy source files to cache dependencies
RUN mkdir -p crates/ci_runner/src && \
    echo "fn main() {}" > crates/ci_runner/src/main.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release --bin ci_runner || true

# Remove dummy source files
RUN rm -rf crates/*/src

# Copy actual source code
COPY crates ./crates

# Touch the source files to ensure they're newer than the cached build
RUN touch crates/ci_runner/src/main.rs

# Build the application in release mode
RUN cargo build --release --bin ci_runner

# ============================================================
# Stage 2: Final production image
# ============================================================
FROM debian:bookworm

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    curl \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install Docker CLI
RUN install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

# Create app user
RUN groupadd -r ci-runner && \
    useradd -r -g ci-runner -d /app -s /sbin/nologin ci-runner

# Create necessary directories
RUN mkdir -p \
    /app \
    /app/workspaces \
    /app/cache \
    /app/configs \
    /var/log/ci-runner \
    && chown -R ci-runner:ci-runner /app

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/ci_runner /usr/local/bin/ci_runner

# Copy default config
COPY config.yaml.example /app/configs/config.yaml

# Set ownership
RUN chown -R ci-runner:ci-runner /app

# Switch to app user
USER ci-runner

# Expose ports
# 8080 - HTTP API
# 9090 - Prometheus metrics
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default environment variables
ENV RUST_LOG=info
ENV CI_CONFIG=/app/configs/config.yaml

# Run the CI runner
ENTRYPOINT ["/usr/local/bin/ci_runner"]
