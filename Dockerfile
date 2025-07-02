# Build stage
FROM golang:1.23.1-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates


# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

ARG VERSION

# Build the binary
RUN go build -ldflags "-w -s -X main.Version=$VERSION" -o querator ./cmd/querator

# Final stage
FROM alpine:latest

LABEL org.opencontainers.image.source="https://github.com/kapetan-io/querator"

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create app user
RUN addgroup -g 1001 -S querator && \
    adduser -u 1001 -S querator -G querator

# Create directories for data and config
RUN mkdir -p /data /config && \
    chown -R querator:querator /data /config

# Copy binary from builder
COPY --from=builder /app/querator /usr/local/bin/querator
COPY --from=builder /app/example.yaml /config/default.yaml

# Set permissions
RUN chmod +x /usr/local/bin/querator

# Switch to non-root user
USER querator

# Set working directory
WORKDIR /data

# Expose port (default querator port)
EXPOSE 2319

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:9090/health || exit 1

# Default command - use default config if no config mounted
CMD ["sh", "-c", "if [ -f /config/querator.yaml ]; then querator server --config /config/querator.yaml; else querator server --config /config/default.yaml; fi"]
