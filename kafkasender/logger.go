package kafkasender

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// LogLevel represents logging levels
type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
)

// Logger wraps slog.Logger with Kafka-specific functionality
type Logger struct {
	*slog.Logger
	level LogLevel
}

// NewLogger creates a new structured logger
func NewLogger(level LogLevel) *Logger {
	var slogLevel slog.Level
	switch level {
	case LevelDebug:
		slogLevel = slog.LevelDebug
	case LevelInfo:
		slogLevel = slog.LevelInfo
	case LevelWarn:
		slogLevel = slog.LevelWarn
	case LevelError:
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: slogLevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Add timestamp formatting
			if a.Key == slog.TimeKey {
				return slog.String("timestamp", a.Value.Time().Format(time.RFC3339))
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)

	return &Logger{
		Logger: logger,
		level:  level,
	}
}

// WithComponent adds component context to logger
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{
		Logger: l.Logger.With("component", component),
		level:  l.level,
	}
}

// WithContext adds request context to logger
func (l *Logger) WithContext(ctx context.Context) *Logger {
	// Extract any context values that should be logged
	logger := l.Logger
	
	if reqID := ctx.Value("request_id"); reqID != nil {
		logger = logger.With("request_id", reqID)
	}
	
	if userID := ctx.Value("user_id"); userID != nil {
		logger = logger.With("user_id", userID)
	}

	return &Logger{
		Logger: logger,
		level:  l.level,
	}
}

// LogProducerStart logs producer initialization
func (l *Logger) LogProducerStart(config *Config) {
	l.Info("Starting Kafka producer",
		"brokers", config.Brokers,
		"client_id", config.ClientID,
		"security_protocol", config.SecurityProtocol,
		"compression", config.ProducerCompression,
		"batch_size", config.ProducerBatchSize,
		"idempotent", config.ProducerIdempotent,
	)
}

// LogProducerStop logs producer shutdown
func (l *Logger) LogProducerStop(metrics ProducerMetrics) {
	l.Info("Stopping Kafka producer",
		"uptime", time.Since(metrics.StartTime),
		"messages_sent", metrics.MessagesSent,
		"messages_failed", metrics.MessagesFailedTotal,
		"avg_latency", metrics.AvgLatency,
	)
}

// LogMessageSent logs successful message delivery
func (l *Logger) LogMessageSent(metadata MessageMetadata, duration time.Duration) {
	l.Debug("Message sent successfully",
		"topic", metadata.Topic,
		"partition", metadata.Partition,
		"offset", metadata.Offset,
		"key", metadata.Key,
		"duration_ms", duration.Milliseconds(),
	)
}

// LogMessageFailed logs failed message delivery
func (l *Logger) LogMessageFailed(topic, key string, err error, duration time.Duration) {
	l.Error("Message send failed",
		"topic", topic,
		"key", key,
		"error", err.Error(),
		"duration_ms", duration.Milliseconds(),
	)
}

// LogCircuitBreakerState logs circuit breaker state changes
func (l *Logger) LogCircuitBreakerState(state string, reason string) {
	l.Warn("Circuit breaker state changed",
		"state", state,
		"reason", reason,
	)
}

// LogRateLimit logs rate limiting events
func (l *Logger) LogRateLimit(topic string, requests int) {
	l.Warn("Rate limit hit",
		"topic", topic,
		"requests_per_second", requests,
	)
}

// LogHealth logs health status
func (l *Logger) LogHealth(status HealthStatus) {
	level := slog.LevelInfo
	if status.Status == "unhealthy" {
		level = slog.LevelError
	} else if status.Status == "degraded" {
		level = slog.LevelWarn
	}

	l.Log(context.Background(), level, "Producer health status",
		"status", status.Status,
		"messages_sent", status.MessagesSent,
		"error_count", status.ErrorCount,
		"last_error", status.LastError,
		"uptime", status.Uptime,
	)
}

// LogMetrics logs performance metrics
func (l *Logger) LogMetrics(metrics ProducerMetrics) {
	l.Info("Producer metrics",
		"messages_sent", metrics.MessagesSent,
		"messages_failed_total", metrics.MessagesFailedTotal,
		"messages_failed_last_minute", metrics.MessagesFailedLast,
		"avg_latency_ms", metrics.AvgLatency.Milliseconds(),
		"max_latency_ms", metrics.MaxLatency.Milliseconds(),
		"circuit_breaker_state", metrics.CircuitBreakerState,
		"rate_limit_hits", metrics.RateLimitHits,
		"uptime", time.Since(metrics.StartTime),
	)
}