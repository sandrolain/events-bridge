# Build stage - Using Debian for better toolchain support (includes gold linker)
FROM golang:1.25-bookworm AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    make \
    gcc \
    g++ \
    binutils-gold \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY src/ ./src/

# Build the main application (CGO not needed for main app)
RUN CGO_ENABLED=0 go build -o events-bridge ./src

# Build connector plugins (CGO required for buildmode=plugin)
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN mkdir -p /build/connectors && \
    echo "Building connector plugins..." && \
    for d in ./src/connectors/*; do \
      [ -d "$d" ] || continue; \
      name="$(basename "$d")"; \
      out="/build/connectors/${name}.so"; \
      echo "Building plugin: $name"; \
      if CGO_ENABLED=1 go build -buildmode=plugin -o "$out" "$d" 2>&1; then \
        echo "  ✓ $name.so built successfully ($(du -h "$out" | cut -f1))"; \
      else \
        echo "  ✗ $name.so build failed (may not be a plugin)"; \
      fi \
    done && \
    echo "Plugin build summary:" && \
    ls -lh /build/connectors/ || echo "No plugins built"

# Runtime stage
FROM alpine:3.22

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 events && \
    adduser -D -u 1000 -G events events

# Set working directory
WORKDIR /app

# Copy binary and plugins from builder
COPY --from=builder /build/events-bridge /app/
COPY --from=builder /build/connectors /app/connectors/

# Create config directory
RUN mkdir -p /app/config && \
    chown -R events:events /app

# Switch to non-root user
USER events

# Default config path (can be overridden)
ENV CONFIG_PATH=/app/config/config.yaml

# Expose common ports (can be overridden based on configuration)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ps aux | grep events-bridge || exit 1

# Run the application
CMD ["/bin/sh", "-c", "/app/events-bridge --config ${CONFIG_PATH}"]
