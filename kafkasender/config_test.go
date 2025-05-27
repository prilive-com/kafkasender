package kafkasender

import (
	"os"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		wantErr  bool
		validate func(*Config) bool
	}{
		{
			name: "valid minimal config",
			envVars: map[string]string{
				"KAFKA_BROKERS": "localhost:9092",
			},
			wantErr: false,
			validate: func(c *Config) bool {
				return len(c.Brokers) == 1 && c.Brokers[0] == "localhost:9092"
			},
		},
		{
			name: "valid SASL config",
			envVars: map[string]string{
				"KAFKA_BROKERS":          "localhost:9092",
				"KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
				"KAFKA_SASL_USERNAME":     "user",
				"KAFKA_SASL_PASSWORD":     "pass",
			},
			wantErr: false,
			validate: func(c *Config) bool {
				return c.SecurityProtocol == "SASL_PLAINTEXT" &&
					c.SASLUsername == "user" &&
					c.SASLPassword == "pass"
			},
		},
		{
			name: "missing brokers",
			envVars: map[string]string{},
			wantErr: true,
		},
		{
			name: "SASL without credentials",
			envVars: map[string]string{
				"KAFKA_BROKERS":          "localhost:9092",
				"KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
			},
			wantErr: true,
		},
		{
			name: "invalid compression",
			envVars: map[string]string{
				"KAFKA_BROKERS":               "localhost:9092",
				"KAFKA_PRODUCER_COMPRESSION": "invalid",
			},
			wantErr: true,
		},
		{
			name: "multiple brokers",
			envVars: map[string]string{
				"KAFKA_BROKERS": "localhost:9092,localhost:9093,localhost:9094",
			},
			wantErr: false,
			validate: func(c *Config) bool {
				return len(c.Brokers) == 3 &&
					c.Brokers[0] == "localhost:9092" &&
					c.Brokers[1] == "localhost:9093" &&
					c.Brokers[2] == "localhost:9094"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			os.Clearenv()

			// Set test environment variables
			for k, v := range tt.envVars {
				os.Setenv(k, v)
			}

			config, err := LoadConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadConfig() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("LoadConfig() unexpected error: %v", err)
				return
			}

			if tt.validate != nil && !tt.validate(config) {
				t.Errorf("LoadConfig() validation failed")
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				Brokers:               []string{"localhost:9092"},
				SecurityProtocol:      "PLAINTEXT",
				ProducerTimeout:       10 * time.Second,
				ProducerFlushFreq:     100 * time.Millisecond,
				ProducerBatchSize:     16384,
				ProducerCompression:   "snappy",
				ProducerRequiredAcks:  "WaitForLocal",
				RateLimitRequests:     1000,
				RateLimitBurst:        2000,
			},
			wantErr: false,
		},
		{
			name: "empty brokers",
			config: &Config{
				Brokers: []string{},
			},
			wantErr: true,
		},
		{
			name: "invalid security protocol",
			config: &Config{
				Brokers:          []string{"localhost:9092"},
				SecurityProtocol: "INVALID",
			},
			wantErr: true,
		},
		{
			name: "invalid timeout",
			config: &Config{
				Brokers:         []string{"localhost:9092"},
				ProducerTimeout: -1 * time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestToSaramaConfig(t *testing.T) {
	config := &Config{
		Brokers:               []string{"localhost:9092"},
		ClientID:              "test-client",
		SecurityProtocol:      "PLAINTEXT",
		ProducerTimeout:       10 * time.Second,
		ProducerRetryMax:      3,
		ProducerFlushFreq:     100 * time.Millisecond,
		ProducerBatchSize:     16384,
		ProducerCompression:   "snappy",
		ProducerRequiredAcks:  "WaitForLocal",
		ProducerIdempotent:    true,
	}

	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		t.Fatalf("ToSaramaConfig() error = %v", err)
	}

	// Verify key settings
	if saramaConfig.ClientID != "test-client" {
		t.Errorf("ClientID = %v, want %v", saramaConfig.ClientID, "test-client")
	}

	if saramaConfig.Producer.RequiredAcks != sarama.WaitForLocal {
		t.Errorf("RequiredAcks = %v, want %v", saramaConfig.Producer.RequiredAcks, sarama.WaitForLocal)
	}

	if saramaConfig.Producer.Compression != sarama.CompressionSnappy {
		t.Errorf("Compression = %v, want %v", saramaConfig.Producer.Compression, sarama.CompressionSnappy)
	}

	if !saramaConfig.Producer.Idempotent {
		t.Errorf("Idempotent = %v, want %v", saramaConfig.Producer.Idempotent, true)
	}
}

func TestCompressionMapping(t *testing.T) {
	tests := []struct {
		input    string
		expected sarama.CompressionCodec
		wantErr  bool
	}{
		{"none", sarama.CompressionNone, false},
		{"snappy", sarama.CompressionSnappy, false},
		{"lz4", sarama.CompressionLZ4, false},
		{"gzip", sarama.CompressionGZIP, false},
		{"zstd", sarama.CompressionZSTD, false},
		{"invalid", sarama.CompressionNone, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			config := &Config{ProducerCompression: tt.input}
			result, err := config.getCompressionCodec()

			if (err != nil) != tt.wantErr {
				t.Errorf("getCompressionCodec() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && result != tt.expected {
				t.Errorf("getCompressionCodec() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSASLMechanismMapping(t *testing.T) {
	tests := []struct {
		input    string
		expected sarama.SASLMechanism
		wantErr  bool
	}{
		{"PLAIN", sarama.SASLTypePlaintext, false},
		{"SCRAM-SHA-256", sarama.SASLTypeSCRAMSHA256, false},
		{"SCRAM-SHA-512", sarama.SASLTypeSCRAMSHA512, false},
		{"invalid", sarama.SASLTypePlaintext, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			config := &Config{SASLMechanism: tt.input}
			result, err := config.getSASLMechanism()

			if (err != nil) != tt.wantErr {
				t.Errorf("getSASLMechanism() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && result != tt.expected {
				t.Errorf("getSASLMechanism() = %v, want %v", result, tt.expected)
			}
		})
	}
}