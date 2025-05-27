package kafkasender

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// Config holds all configuration for the Kafka producer
type Config struct {
	// Core Kafka settings
	Brokers  []string `json:"brokers"`
	ClientID string   `json:"client_id"`

	// Security
	SecurityProtocol string `json:"security_protocol"` // PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL
	SASLMechanism    string `json:"sasl_mechanism"`    // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
	SASLUsername     string `json:"sasl_username"`
	SASLPassword     string `json:"sasl_password"`

	// Producer settings
	ProducerTimeout       time.Duration `json:"producer_timeout"`
	ProducerRetryMax      int           `json:"producer_retry_max"`
	ProducerFlushFreq     time.Duration `json:"producer_flush_frequency"`
	ProducerBatchSize     int           `json:"producer_batch_size"`
	ProducerCompression   string        `json:"producer_compression"` // none, snappy, lz4, gzip, zstd
	ProducerRequiredAcks  string        `json:"producer_required_acks"` // NoResponse, WaitForLocal, WaitForAll
	ProducerIdempotent    bool          `json:"producer_idempotent"`

	// Rate limiting
	RateLimitRequests int `json:"rate_limit_requests"`
	RateLimitBurst    int `json:"rate_limit_burst"`

	// Circuit breaker
	BreakerMaxRequests uint32        `json:"breaker_max_requests"`
	BreakerInterval    time.Duration `json:"breaker_interval"`
	BreakerTimeout     time.Duration `json:"breaker_timeout"`

	// Logging
	LogLevel string `json:"log_level"` // DEBUG, INFO, WARN, ERROR
}

// LoadConfig loads configuration from environment variables with sensible defaults
func LoadConfig() (*Config, error) {
	config := &Config{
		// Defaults for production use
		ClientID:              getEnvOrDefault("KAFKA_CLIENT_ID", "kafkasender-client"),
		SecurityProtocol:      getEnvOrDefault("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
		SASLMechanism:         getEnvOrDefault("KAFKA_SASL_MECHANISM", "PLAIN"),
		ProducerTimeout:       parseDurationOrDefault("KAFKA_PRODUCER_TIMEOUT", 10*time.Second),
		ProducerRetryMax:      parseIntOrDefault("KAFKA_PRODUCER_RETRY_MAX", 3),
		ProducerFlushFreq:     parseDurationOrDefault("KAFKA_PRODUCER_FLUSH_FREQUENCY", 100*time.Millisecond),
		ProducerBatchSize:     parseIntOrDefault("KAFKA_PRODUCER_BATCH_SIZE", 16384), // 16KB
		ProducerCompression:   getEnvOrDefault("KAFKA_PRODUCER_COMPRESSION", "snappy"),
		ProducerRequiredAcks:  getEnvOrDefault("KAFKA_PRODUCER_REQUIRED_ACKS", "WaitForLocal"),
		ProducerIdempotent:    parseBoolOrDefault("KAFKA_PRODUCER_IDEMPOTENT", true),
		RateLimitRequests:     parseIntOrDefault("KAFKA_RATE_LIMIT_REQUESTS", 1000),
		RateLimitBurst:        parseIntOrDefault("KAFKA_RATE_LIMIT_BURST", 2000),
		BreakerMaxRequests:    uint32(parseIntOrDefault("KAFKA_BREAKER_MAX_REQUESTS", 5)),
		BreakerInterval:       parseDurationOrDefault("KAFKA_BREAKER_INTERVAL", 2*time.Minute),
		BreakerTimeout:        parseDurationOrDefault("KAFKA_BREAKER_TIMEOUT", 60*time.Second),
		LogLevel:              getEnvOrDefault("KAFKA_LOG_LEVEL", "INFO"),
	}

	// Required fields
	brokersStr := os.Getenv("KAFKA_BROKERS")
	if brokersStr == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS environment variable is required")
	}
	config.Brokers = strings.Split(brokersStr, ",")
	for i, broker := range config.Brokers {
		config.Brokers[i] = strings.TrimSpace(broker)
	}

	// SASL credentials (required if using SASL)
	if strings.Contains(config.SecurityProtocol, "SASL") {
		config.SASLUsername = os.Getenv("KAFKA_SASL_USERNAME")
		config.SASLPassword = os.Getenv("KAFKA_SASL_PASSWORD")
		if config.SASLUsername == "" || config.SASLPassword == "" {
			return nil, fmt.Errorf("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for SASL authentication")
		}
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate brokers
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker must be specified")
	}

	// Validate security protocol
	validSecurityProtocols := map[string]bool{
		"PLAINTEXT":      true,
		"SASL_PLAINTEXT": true,
		"SASL_SSL":       true,
		"SSL":            true,
	}
	if !validSecurityProtocols[c.SecurityProtocol] {
		return fmt.Errorf("invalid security protocol: %s", c.SecurityProtocol)
	}

	// Validate SASL mechanism
	validSASLMechanisms := map[string]bool{
		"PLAIN":         true,
		"SCRAM-SHA-256": true,
		"SCRAM-SHA-512": true,
	}
	if strings.Contains(c.SecurityProtocol, "SASL") && !validSASLMechanisms[c.SASLMechanism] {
		return fmt.Errorf("invalid SASL mechanism: %s", c.SASLMechanism)
	}

	// Validate compression
	validCompressions := map[string]bool{
		"none":   true,
		"snappy": true,
		"lz4":    true,
		"gzip":   true,
		"zstd":   true,
	}
	if !validCompressions[c.ProducerCompression] {
		return fmt.Errorf("invalid compression: %s", c.ProducerCompression)
	}

	// Validate required acks
	validAcks := map[string]bool{
		"NoResponse":   true,
		"WaitForLocal": true,
		"WaitForAll":   true,
	}
	if !validAcks[c.ProducerRequiredAcks] {
		return fmt.Errorf("invalid required acks: %s", c.ProducerRequiredAcks)
	}

	// Validate timeouts
	if c.ProducerTimeout <= 0 {
		return fmt.Errorf("producer timeout must be positive")
	}
	if c.ProducerFlushFreq <= 0 {
		return fmt.Errorf("producer flush frequency must be positive")
	}

	// Validate batch size
	if c.ProducerBatchSize <= 0 {
		return fmt.Errorf("producer batch size must be positive")
	}

	// Validate rate limiting
	if c.RateLimitRequests <= 0 {
		return fmt.Errorf("rate limit requests must be positive")
	}
	if c.RateLimitBurst <= 0 {
		return fmt.Errorf("rate limit burst must be positive")
	}

	return nil
}

// ToSaramaConfig converts our config to Sarama producer config
func (c *Config) ToSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()

	// Producer settings
	config.Producer.RequiredAcks = c.getRequiredAcks()
	config.Producer.Retry.Max = c.ProducerRetryMax
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Timeout = c.ProducerTimeout
	config.Producer.Flush.Frequency = c.ProducerFlushFreq
	config.Producer.Flush.Messages = c.ProducerBatchSize
	config.Producer.Idempotent = c.ProducerIdempotent

	// Compression
	compression, err := c.getCompressionCodec()
	if err != nil {
		return nil, err
	}
	config.Producer.Compression = compression

	// Client ID
	config.ClientID = c.ClientID

	// Security
	if strings.Contains(c.SecurityProtocol, "SASL") {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SASLUsername
		config.Net.SASL.Password = c.SASLPassword

		mechanism, err := c.getSASLMechanism()
		if err != nil {
			return nil, err
		}
		config.Net.SASL.Mechanism = mechanism
	}

	// Version (use latest stable)
	config.Version = sarama.V2_8_1_0

	return config, nil
}

// getRequiredAcks converts string to sarama.RequiredAcks
func (c *Config) getRequiredAcks() sarama.RequiredAcks {
	switch c.ProducerRequiredAcks {
	case "NoResponse":
		return sarama.NoResponse
	case "WaitForLocal":
		return sarama.WaitForLocal
	case "WaitForAll":
		return sarama.WaitForAll
	default:
		return sarama.WaitForLocal
	}
}

// getCompressionCodec converts string to sarama.CompressionCodec
func (c *Config) getCompressionCodec() (sarama.CompressionCodec, error) {
	switch c.ProducerCompression {
	case "none":
		return sarama.CompressionNone, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "zstd":
		return sarama.CompressionZSTD, nil
	default:
		return sarama.CompressionNone, fmt.Errorf("unsupported compression: %s", c.ProducerCompression)
	}
}

// getSASLMechanism converts string to sarama.SASLMechanism
func (c *Config) getSASLMechanism() (sarama.SASLMechanism, error) {
	switch c.SASLMechanism {
	case "PLAIN":
		return sarama.SASLTypePlaintext, nil
	case "SCRAM-SHA-256":
		return sarama.SASLTypeSCRAMSHA256, nil
	case "SCRAM-SHA-512":
		return sarama.SASLTypeSCRAMSHA512, nil
	default:
		return sarama.SASLTypePlaintext, fmt.Errorf("unsupported SASL mechanism: %s", c.SASLMechanism)
	}
}

// Utility functions for parsing environment variables
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func parseBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func parseDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}