FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o proxy ./cmd/main.go

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates curl

WORKDIR /app

# Copy built binary
COPY --from=builder /app/proxy .
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/wasm_scripts ./wasm_scripts
COPY --from=builder /app/certs ./certs

# Expose ports
EXPOSE 9042 8080 9090

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:8080/healthz/ || exit 1

# Run the proxy
CMD ["./proxy"]