# Build stage
FROM golang:1.24-alpine AS builder

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

# Final stage - use distroless for smaller image and better cross-platform support
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/kapetan-io/querator"

# Copy binary and config from builder
COPY --from=builder /app/querator /usr/local/bin/querator
COPY --from=builder /app/example.yaml /config/default.yaml

# Expose port (default querator port)
EXPOSE 2319

# Run as nonroot user (provided by distroless)
USER nonroot:nonroot

# Set working directory
WORKDIR /data

# Default command
ENTRYPOINT ["/usr/local/bin/querator", "server", "--config", "/config/default.yaml"]
