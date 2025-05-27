# kafkasender

**kafkasender** is a production-ready, high-performance Go library for sending messages to Apache Kafka with built-in resilience, observability, and performance optimization features.

## ✨ Features

| Capability | Details |
|------------|---------|
| **Production Ready** | Battle-tested patterns with circuit breakers, rate limiting, and retry logic |
| **High Performance** | Async/sync modes, connection pooling, efficient batching, compression |
| **Resilient** | Circuit breaker pattern, exponential backoff, graceful degradation |
| **Observable** | Structured logging (slog), health checks, performance metrics |
| **Secure** | SASL authentication support (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| **Configurable** | Environment-based configuration with validation and sensible defaults |
| **Type Safe** | Clean interfaces, comprehensive error handling, context support |

## 📦 Installation

```bash
go get github.com/prilive-com/kafkasender
```

## 🚀 Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "log"
    
    "github.com/prilive-com/kafkasender/kafkasender"
)

func main() {
    // Load configuration from environment variables
    config, err := kafkasender.LoadConfig()
    if err != nil {
        log.Fatal(err)
    }

    // Create producer
    producer, err := kafkasender.NewProducer(config, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // Send a message
    ctx := context.Background()
    err = producer.SendMessage(ctx, "my-topic", "my-key", "Hello, Kafka!")
    if err != nil {
        log.Printf("Failed to send message: %v", err)
    }
}
```

### Environment Configuration

Set these environment variables:

```bash
# Required
KAFKA_BROKERS=localhost:9092,localhost:9093

# Optional (with defaults)
KAFKA_CLIENT_ID=kafkasender-client
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_PRODUCER_COMPRESSION=snappy
KAFKA_PRODUCER_BATCH_SIZE=16384
KAFKA_RATE_LIMIT_REQUESTS=1000
KAFKA_LOG_LEVEL=INFO
```

For SASL authentication:
```bash
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=your-username
KAFKA_SASL_PASSWORD=your-password
```

## 🏗️ Architecture

```
┌──────────────┐   Message Request    ┌──────────────┐   HTTP Client   ┌────────┐
│Your App Logic│ ────────────────────▶│ KafkaProducer │ ───────────────▶│ Kafka  │
└──────────────┘                      │ (rate‑limit) │                 └────────┘
                                      │ (circuit‑br) │
                                      │ (retry)      │
                                      │ (metrics)    │
                                      └──────────────┘
```

### Core Components

- **Config**: Environment-based configuration with validation
- **Producer**: Main message sending interface with resilience features
- **Logger**: Structured logging with performance insights
- **Circuit Breaker**: Prevents cascade failures using sony/gobreaker
- **Rate Limiter**: Token bucket rate limiting using golang.org/x/time

## 📋 Configuration Reference

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | *(required)* | Comma-separated list of Kafka brokers |
| `KAFKA_CLIENT_ID` | `kafkasender-client` | Client identifier |

### Security

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol (PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL) |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| `KAFKA_SASL_USERNAME` | | SASL username (required for SASL protocols) |
| `KAFKA_SASL_PASSWORD` | | SASL password (required for SASL protocols) |

### Producer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_PRODUCER_TIMEOUT` | `10s` | Producer timeout |
| `KAFKA_PRODUCER_RETRY_MAX` | `3` | Maximum retry attempts |
| `KAFKA_PRODUCER_FLUSH_FREQUENCY` | `100ms` | Flush frequency for batching |
| `KAFKA_PRODUCER_BATCH_SIZE` | `16384` | Batch size in bytes |
| `KAFKA_PRODUCER_COMPRESSION` | `snappy` | Compression (none, snappy, lz4, gzip, zstd) |
| `KAFKA_PRODUCER_REQUIRED_ACKS` | `WaitForLocal` | Required acknowledgments (NoResponse, WaitForLocal, WaitForAll) |
| `KAFKA_PRODUCER_IDEMPOTENT` | `true` | Enable idempotent producer |

### Resilience

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_RATE_LIMIT_REQUESTS` | `1000` | Requests per second limit |
| `KAFKA_RATE_LIMIT_BURST` | `2000` | Burst token bucket size |
| `KAFKA_BREAKER_MAX_REQUESTS` | `5` | Circuit breaker half-open requests |
| `KAFKA_BREAKER_INTERVAL` | `2m` | Circuit breaker reset interval |
| `KAFKA_BREAKER_TIMEOUT` | `60s` | Circuit breaker open timeout |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARN, ERROR) |

## 🔧 Advanced Usage

### Custom Options

```go
options := &kafkasender.ProducerOptions{
    AsyncMode:       true,  // Fire-and-forget mode
    AsyncBufferSize: 10000, // Async buffer size
    
    ErrorHandler: func(err error) {
        log.Printf("Kafka error: %v", err)
    },
    
    SuccessHandler: func(metadata kafkasender.MessageMetadata) {
        log.Printf("Message sent: %+v", metadata)
    },
    
    MetricsHandler: func(metrics kafkasender.ProducerMetrics) {
        log.Printf("Producer metrics: %+v", metrics)
    },
}

producer, err := kafkasender.NewProducer(config, options)
```

### Sending User Registration Data

```go
userData := kafkasender.UserRegistrationData{
    Name:             "John Doe",
    Email:            "john@example.com",
    TelegramUserID:   123456789,
    TelegramUsername: "johndoe",
    FirstName:        "John",
    LastName:         "Doe",
    LanguageCode:     "en",
    Source:           "telegram-bot",
}

err := producer.SendUserRegistration(ctx, "user-registration", userData)
```

### Sending with Custom Headers

```go
headers := map[string]string{
    "content-type": "application/json",
    "version":      "1.0",
    "trace-id":     "abc123",
}

err := producer.SendMessageWithHeaders(ctx, "my-topic", "my-key", data, headers)
```

### Health Monitoring

```go
health := producer.Health()
fmt.Printf("Status: %s\n", health.Status)         // healthy, degraded, unhealthy
fmt.Printf("Messages Sent: %d\n", health.MessagesSent)
fmt.Printf("Error Count: %d\n", health.ErrorCount)
fmt.Printf("Uptime: %v\n", health.Uptime)
```

## 🧪 Testing

Run the test suite:

```bash
cd kafkasender
go test ./...
```

Run with coverage:

```bash
go test -cover ./...
```

Run integration tests (requires running Kafka):

```bash
# Set up test environment
export KAFKA_BROKERS=localhost:9092
export KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# Run tests
go test -tags integration ./...
```

## 📊 Performance

### Benchmarks

```bash
go test -bench=. -benchmem ./...
```

### Typical Performance

- **Throughput**: 10,000+ messages/second (depending on message size and Kafka setup)
- **Latency**: Sub-millisecond in async mode, 1-5ms in sync mode
- **Memory**: Low allocation design with object pooling
- **CPU**: Efficient batching and compression

### Optimization Tips

1. **Use async mode** for maximum throughput
2. **Enable compression** (snappy recommended for balance)
3. **Tune batch size** based on message size
4. **Use appropriate acknowledgment level** (WaitForLocal for most cases)
5. **Monitor circuit breaker** and rate limiter metrics

## 🐳 Docker Support

Create a Dockerfile:

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o kafkasender-app ./examples

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/kafkasender-app .
CMD ["./kafkasender-app"]
```

## 🔍 Monitoring

### Structured Logs

All logs are JSON formatted with consistent fields:

```json
{
  "timestamp": "2025-05-24T10:30:00Z",
  "level": "INFO",
  "component": "kafkasender",
  "msg": "Message sent successfully",
  "topic": "user-registration",
  "partition": 0,
  "offset": 12345,
  "duration_ms": 15
}
```

### Metrics Integration

Use the metrics handler for Prometheus integration:

```go
options.MetricsHandler = func(metrics kafkasender.ProducerMetrics) {
    // Export to Prometheus, StatsD, etc.
    prometheusMessagesSent.Set(float64(metrics.MessagesSent))
    prometheusAvgLatency.Set(float64(metrics.AvgLatency.Milliseconds()))
}
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

MIT © 2025 Prilive Com

## 🙏 Acknowledgments

- Built on top of [IBM Sarama](https://github.com/IBM/sarama)
- Circuit breaker by [Sony GoBreaker](https://github.com/sony/gobreaker)
- Rate limiting by [golang.org/x/time](https://pkg.go.dev/golang.org/x/time/rate)