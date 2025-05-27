package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prilive-com/kafkasender/kafkasender"
)

func main() {
	// Load configuration from environment variables
	config, err := kafkasender.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create producer options
	options := &kafkasender.ProducerOptions{
		AsyncMode:       false, // Synchronous mode for this example
		AsyncBufferSize: 1000,
		ErrorHandler: func(err error) {
			fmt.Printf("Error sending message: %v\n", err)
		},
		SuccessHandler: func(metadata kafkasender.MessageMetadata) {
			fmt.Printf("Message sent successfully: topic=%s, partition=%d, offset=%d\n",
				metadata.Topic, metadata.Partition, metadata.Offset)
		},
		MetricsHandler: func(metrics kafkasender.ProducerMetrics) {
			fmt.Printf("Metrics: sent=%d, failed=%d, avg_latency=%v\n",
				metrics.MessagesSent, metrics.MessagesFailedTotal, metrics.AvgLatency)
		},
	}

	// Create producer
	producer, err := kafkasender.NewProducer(config, options)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Example 1: Send simple text message
	fmt.Println("Example 1: Sending simple text message")
	err = producer.SendMessage(ctx, "test-topic", "key1", "Hello, Kafka!")
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
	}

	// Example 2: Send JSON message with headers
	fmt.Println("Example 2: Sending JSON message with headers")
	data := map[string]interface{}{
		"id":        123,
		"name":      "John Doe",
		"timestamp": time.Now(),
	}
	headers := map[string]string{
		"content-type": "application/json",
		"version":      "1.0",
	}
	err = producer.SendMessageWithHeaders(ctx, "test-topic", "key2", data, headers)
	if err != nil {
		fmt.Printf("Failed to send message: %v\n", err)
	}

	// Example 3: Send user registration data
	fmt.Println("Example 3: Sending user registration data")
	userData := kafkasender.UserRegistrationData{
		Name:             "Alice Johnson",
		Email:            "alice@example.com",
		TelegramUserID:   123456789,
		TelegramUsername: "alice_j",
		FirstName:        "Alice",
		LastName:         "Johnson",
		LanguageCode:     "en",
		Source:           "telegram-bot",
	}
	err = producer.SendUserRegistration(ctx, "user-registration", userData)
	if err != nil {
		fmt.Printf("Failed to send user registration: %v\n", err)
	}

	// Example 4: Send multiple messages (load testing)
	fmt.Println("Example 4: Sending multiple messages (load test)")
	start := time.Now()
	messageCount := 100

	for i := 0; i < messageCount; i++ {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled, stopping load test")
			break
		default:
		}

		message := fmt.Sprintf("Load test message #%d", i+1)
		key := fmt.Sprintf("load-test-%d", i+1)
		
		err := producer.SendMessage(ctx, "load-test-topic", key, message)
		if err != nil {
			fmt.Printf("Failed to send load test message %d: %v\n", i+1, err)
		}

		// Small delay to avoid overwhelming
		time.Sleep(10 * time.Millisecond)
	}

	duration := time.Since(start)
	fmt.Printf("Sent %d messages in %v (%.2f messages/sec)\n", 
		messageCount, duration, float64(messageCount)/duration.Seconds())

	// Show health status
	health := producer.Health()
	fmt.Printf("\nProducer Health Status:\n")
	fmt.Printf("  Status: %s\n", health.Status)
	fmt.Printf("  Messages Sent: %d\n", health.MessagesSent)
	fmt.Printf("  Error Count: %d\n", health.ErrorCount)
	fmt.Printf("  Uptime: %v\n", health.Uptime)
	if health.LastError != "" {
		fmt.Printf("  Last Error: %s\n", health.LastError)
	}

	// Wait for a moment to see any async results
	fmt.Println("\nWaiting 2 seconds for any pending operations...")
	time.Sleep(2 * time.Second)

	// Graceful shutdown
	fmt.Println("Closing producer...")
	if err := producer.Close(); err != nil {
		fmt.Printf("Error closing producer: %v\n", err)
	}

	fmt.Println("Example completed successfully!")
}

// setupEnvironment sets up example environment variables
func setupEnvironment() {
	// Only set if not already configured
	if os.Getenv("KAFKA_BROKERS") == "" {
		os.Setenv("KAFKA_BROKERS", "localhost:9092")
	}
	if os.Getenv("KAFKA_CLIENT_ID") == "" {
		os.Setenv("KAFKA_CLIENT_ID", "kafkasender-example")
	}
	if os.Getenv("KAFKA_LOG_LEVEL") == "" {
		os.Setenv("KAFKA_LOG_LEVEL", "INFO")
	}
}

func init() {
	setupEnvironment()
}