package kafkasender

import (
	"context"
	"time"
)

// Producer defines the interface for sending messages to Kafka
type Producer interface {
	// SendMessage sends a message to the specified topic
	SendMessage(ctx context.Context, topic string, key string, message interface{}) error

	// SendMessageWithHeaders sends a message with custom headers
	SendMessageWithHeaders(ctx context.Context, topic string, key string, message interface{}, headers map[string]string) error

	// SendUserRegistration is a convenience method for sending user registration data
	SendUserRegistration(ctx context.Context, topic string, userData UserRegistrationData) error

	// Close gracefully shuts down the producer
	Close() error

	// Health returns the health status of the producer
	Health() HealthStatus
}

// UserRegistrationData represents user registration information
type UserRegistrationData struct {
	Name             string    `json:"name"`
	Email            string    `json:"email"`
	TelegramUserID   int64     `json:"telegram_user_id"`
	TelegramUsername string    `json:"telegram_username,omitempty"`
	FirstName        string    `json:"first_name,omitempty"`
	LastName         string    `json:"last_name,omitempty"`
	PhoneNumber      string    `json:"phone_number,omitempty"`
	LanguageCode     string    `json:"language_code,omitempty"`
	RegistrationTime time.Time `json:"registration_time"`
	Source           string    `json:"source"`
}

// MessageMetadata contains metadata about sent messages
type MessageMetadata struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// HealthStatus represents the health of the producer
type HealthStatus struct {
	Status      string    `json:"status"` // healthy, degraded, unhealthy
	LastSuccess time.Time `json:"last_success"`
	LastError   string    `json:"last_error,omitempty"`
	ErrorCount  int64     `json:"error_count"`
	MessagesSent int64    `json:"messages_sent"`
	Uptime      time.Duration `json:"uptime"`
}

// ProducerMetrics contains performance metrics
type ProducerMetrics struct {
	MessagesSent        int64         `json:"messages_sent"`
	MessagesFailedTotal int64         `json:"messages_failed_total"`
	MessagesFailedLast  int64         `json:"messages_failed_last_minute"`
	AvgLatency          time.Duration `json:"avg_latency"`
	MaxLatency          time.Duration `json:"max_latency"`
	CircuitBreakerState string        `json:"circuit_breaker_state"`
	RateLimitHits       int64         `json:"rate_limit_hits"`
	StartTime           time.Time     `json:"start_time"`
}

// MessageResult represents the result of sending a message
type MessageResult struct {
	Success  bool              `json:"success"`
	Metadata *MessageMetadata  `json:"metadata,omitempty"`
	Error    error             `json:"error,omitempty"`
	Duration time.Duration     `json:"duration"`
}

// ProducerOptions contains optional settings for creating a producer
type ProducerOptions struct {
	// Custom error handler (optional)
	ErrorHandler func(error)
	
	// Custom success handler (optional)
	SuccessHandler func(MessageMetadata)
	
	// Custom metrics handler (optional)
	MetricsHandler func(ProducerMetrics)
	
	// Enable async mode (fire and forget)
	AsyncMode bool
	
	// Buffer size for async operations
	AsyncBufferSize int
}