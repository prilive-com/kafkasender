package kafkasender

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/sony/gobreaker"
	"golang.org/x/time/rate"
)

// KafkaProducer implements the Producer interface
type KafkaProducer struct {
	// Core components
	producer sarama.AsyncProducer
	config   *Config
	logger   *Logger

	// Resilience components
	circuitBreaker *gobreaker.CircuitBreaker
	rateLimiter    *rate.Limiter

	// Metrics and health tracking
	metrics      *ProducerMetrics
	health       *HealthStatus
	metricsLock  sync.RWMutex
	healthLock   sync.RWMutex

	// Sync operation tracking (Option 2: Separate Channels + Option 3: Message ID Tracking)
	syncPendingMessages map[string]chan MessageResult // messageID -> result channel
	syncMutex          sync.RWMutex

	// Lifecycle management
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closed     int32
	startTime  time.Time

	// Optional handlers
	options *ProducerOptions
}

// NewProducer creates a new Kafka producer with the given configuration
func NewProducer(config *Config, options *ProducerOptions) (*KafkaProducer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if options == nil {
		options = &ProducerOptions{
			AsyncBufferSize: 1000,
		}
	}

	// Create logger
	logger := NewLogger(LogLevel(config.LogLevel)).WithComponent("kafkasender")
	
	// Create Sarama configuration
	saramaConfig, err := config.ToSaramaConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create Sarama config: %w", err)
	}

	// Create producer
	producer, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create context for lifecycle management
	ctx, cancel := context.WithCancel(context.Background())

	// Create circuit breaker
	circuitBreakerSettings := gobreaker.Settings{
		Name:        "kafka-producer",
		MaxRequests: config.BreakerMaxRequests,
		Interval:    config.BreakerInterval,
		Timeout:     config.BreakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.LogCircuitBreakerState(to.String(), fmt.Sprintf("changed from %s", from.String()))
		},
	}

	circuitBreaker := gobreaker.NewCircuitBreaker(circuitBreakerSettings)

	// Create rate limiter
	rateLimiter := rate.NewLimiter(rate.Limit(config.RateLimitRequests), config.RateLimitBurst)

	// Initialize metrics and health
	startTime := time.Now()
	metrics := &ProducerMetrics{
		StartTime: startTime,
	}
	health := &HealthStatus{
		Status:      "healthy",
		LastSuccess: startTime,
		Uptime:      0,
	}

	kafkaProducer := &KafkaProducer{
		producer:            producer,
		config:              config,
		logger:              logger,
		circuitBreaker:      circuitBreaker,
		rateLimiter:         rateLimiter,
		metrics:             metrics,
		health:              health,
		syncPendingMessages: make(map[string]chan MessageResult),
		ctx:                 ctx,
		cancel:              cancel,
		startTime:           startTime,
		options:             options,
	}

	// Start background goroutines
	kafkaProducer.startBackgroundWorkers()

	logger.LogProducerStart(config)

	return kafkaProducer, nil
}

// SendMessage sends a message to the specified topic
func (p *KafkaProducer) SendMessage(ctx context.Context, topic string, key string, message interface{}) error {
	return p.SendMessageWithHeaders(ctx, topic, key, message, nil)
}

// SendMessageWithHeaders sends a message with custom headers
func (p *KafkaProducer) SendMessageWithHeaders(ctx context.Context, topic string, key string, message interface{}, headers map[string]string) error {
	if atomic.LoadInt32(&p.closed) == 1 {
		return fmt.Errorf("producer is closed")
	}

	startTime := time.Now()

	// Check rate limiting
	if !p.rateLimiter.Allow() {
		atomic.AddInt64(&p.metrics.RateLimitHits, 1)
		p.logger.LogRateLimit(topic, p.config.RateLimitRequests)
		return fmt.Errorf("rate limit exceeded")
	}

	// Serialize message
	payload, err := p.serializeMessage(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Execute through circuit breaker
	result, err := p.circuitBreaker.Execute(func() (interface{}, error) {
		return p.sendMessageInternal(ctx, topic, key, payload, headers, startTime)
	})

	if err != nil {
		p.updateMetricsOnError()
		p.updateHealthOnError(err)
		return err
	}

	// Update metrics on success
	metadata := result.(MessageMetadata)
	p.updateMetricsOnSuccess(time.Since(startTime))
	p.updateHealthOnSuccess()

	// Call success handler if provided
	if p.options.SuccessHandler != nil {
		p.options.SuccessHandler(metadata)
	}

	return nil
}

// SendUserRegistration is a convenience method for sending user registration data
func (p *KafkaProducer) SendUserRegistration(ctx context.Context, topic string, userData UserRegistrationData) error {
	// Set registration time if not already set
	if userData.RegistrationTime.IsZero() {
		userData.RegistrationTime = time.Now()
	}

	// Use telegram_user_id as the key for partitioning
	key := fmt.Sprintf("user_%d", userData.TelegramUserID)

	// Add headers for message identification
	headers := map[string]string{
		"message_type": "user_registration",
		"version":      "1.0",
		"source":       userData.Source,
	}

	return p.SendMessageWithHeaders(ctx, topic, key, userData, headers)
}

// sendMessageInternal handles the actual message sending
func (p *KafkaProducer) sendMessageInternal(ctx context.Context, topic string, key string, payload []byte, headers map[string]string, startTime time.Time) (MessageMetadata, error) {
	// Create Sarama message
	saramaMessage := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(payload),
	}

	// Add headers
	if headers != nil {
		saramaMessage.Headers = make([]sarama.RecordHeader, 0, len(headers))
		for k, v := range headers {
			saramaMessage.Headers = append(saramaMessage.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
	}

	// Add metadata for tracking
	messageID := p.generateMessageID()
	saramaMessage.Metadata = messageID

	// Send message (async)
	select {
	case p.producer.Input() <- saramaMessage:
		// Message queued successfully
	case <-ctx.Done():
		return MessageMetadata{}, fmt.Errorf("context cancelled while sending message")
	case <-time.After(p.config.ProducerTimeout):
		return MessageMetadata{}, fmt.Errorf("timeout while sending message")
	}

	// Wait for delivery confirmation (if not in async mode)
	if !p.options.AsyncMode {
		return p.waitForDelivery(ctx, messageID, topic, key, startTime)
	}

	// In async mode, return immediately with basic metadata
	return MessageMetadata{
		Topic:     topic,
		Key:       key,
		Headers:   headers,
		Timestamp: time.Now(),
	}, nil
}

// waitForDelivery waits for message delivery confirmation using dedicated channels
func (p *KafkaProducer) waitForDelivery(ctx context.Context, messageID string, topic string, key string, startTime time.Time) (MessageMetadata, error) {
	// Create dedicated channel for this sync operation (Option 2: Separate Channels)
	resultChan := make(chan MessageResult, 1)
	
	// Register this sync operation (Option 3: Message ID Tracking)
	p.syncMutex.Lock()
	p.syncPendingMessages[messageID] = resultChan
	p.syncMutex.Unlock()
	
	// Cleanup when done
	defer func() {
		p.syncMutex.Lock()
		delete(p.syncPendingMessages, messageID)
		p.syncMutex.Unlock()
		close(resultChan)
	}()

	timeout := time.After(p.config.ProducerTimeout)

	select {
	case result := <-resultChan:
		if result.Error != nil {
			duration := time.Since(startTime)
			p.logger.LogMessageFailed(topic, key, result.Error, duration)
			return MessageMetadata{}, fmt.Errorf("message delivery failed: %w", result.Error)
		}
		if result.Metadata != nil {
			p.logger.LogMessageSent(*result.Metadata, time.Since(startTime))
			return *result.Metadata, nil
		}
		return MessageMetadata{}, fmt.Errorf("received invalid result")
	case <-ctx.Done():
		return MessageMetadata{}, fmt.Errorf("context cancelled while waiting for delivery")
	case <-timeout:
		return MessageMetadata{}, fmt.Errorf("timeout waiting for delivery confirmation")
	}
}

// Close gracefully shuts down the producer
func (p *KafkaProducer) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil // Already closed
	}

	p.logger.Info("Shutting down Kafka producer")

	// Cancel context to stop background workers
	p.cancel()

	// Close producer
	if err := p.producer.Close(); err != nil {
		p.logger.Error("Error closing producer", "error", err)
	}

	// Wait for background workers to finish
	p.wg.Wait()

	// Log final metrics
	p.metricsLock.RLock()
	finalMetrics := *p.metrics
	p.metricsLock.RUnlock()

	p.logger.LogProducerStop(finalMetrics)

	return nil
}

// Health returns the current health status
func (p *KafkaProducer) Health() HealthStatus {
	p.healthLock.RLock()
	defer p.healthLock.RUnlock()

	status := *p.health
	status.Uptime = time.Since(p.startTime)
	return status
}

// startBackgroundWorkers starts goroutines for message routing and handling
func (p *KafkaProducer) startBackgroundWorkers() {
	// Message Router - handles both sync and async operations (Options 2 + 3)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case success := <-p.producer.Successes():
				messageID := success.Metadata.(string)
				
				// Check if this is a sync operation (Option 3: Message ID Tracking)
				p.syncMutex.RLock()
				syncChan, isSyncOperation := p.syncPendingMessages[messageID]
				p.syncMutex.RUnlock()
				
				if isSyncOperation {
					// Route to sync channel (Option 2: Separate Channels)
					metadata := MessageMetadata{
						Topic:     success.Topic,
						Partition: success.Partition,
						Offset:    success.Offset,
						Timestamp: success.Timestamp,
					}
					select {
					case syncChan <- MessageResult{Success: true, Metadata: &metadata, Error: nil}:
					case <-p.ctx.Done():
						return
					}
				} else {
					// Handle async operation
					if p.options.AsyncMode {
						p.updateMetricsOnSuccess(0)
						p.updateHealthOnSuccess()
						
						if p.options.SuccessHandler != nil {
							metadata := MessageMetadata{
								Topic:     success.Topic,
								Partition: success.Partition,
								Offset:    success.Offset,
								Timestamp: success.Timestamp,
							}
							p.options.SuccessHandler(metadata)
						}
					}
				}
			case <-p.ctx.Done():
				return
			}
		}
	}()

	// Error Router - handles both sync and async error operations
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case err := <-p.producer.Errors():
				messageID := err.Msg.Metadata.(string)
				
				// Check if this is a sync operation (Option 3: Message ID Tracking)
				p.syncMutex.RLock()
				syncChan, isSyncOperation := p.syncPendingMessages[messageID]
				p.syncMutex.RUnlock()
				
				if isSyncOperation {
					// Route to sync channel (Option 2: Separate Channels)
					select {
					case syncChan <- MessageResult{Success: false, Metadata: nil, Error: err.Err}:
					case <-p.ctx.Done():
						return
					}
				} else {
					// Handle async operation
					if p.options.AsyncMode {
						p.updateMetricsOnError()
						p.updateHealthOnError(err.Err)
						
						if p.options.ErrorHandler != nil {
							p.options.ErrorHandler(err.Err)
						}
					}
				}
			case <-p.ctx.Done():
				return
			}
		}
	}()

	// Metrics reporter (if handler provided)
	if p.options.MetricsHandler != nil {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					p.metricsLock.RLock()
					metrics := *p.metrics
					p.metricsLock.RUnlock()
					p.options.MetricsHandler(metrics)
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}
}

// serializeMessage converts a message to JSON bytes
func (p *KafkaProducer) serializeMessage(message interface{}) ([]byte, error) {
	switch v := message.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(message)
	}
}

// generateMessageID generates a unique message ID for tracking
func (p *KafkaProducer) generateMessageID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// updateMetricsOnSuccess updates metrics after successful message delivery
func (p *KafkaProducer) updateMetricsOnSuccess(duration time.Duration) {
	p.metricsLock.Lock()
	defer p.metricsLock.Unlock()

	atomic.AddInt64(&p.metrics.MessagesSent, 1)

	if duration > 0 {
		// Update latency metrics (simple moving average)
		if p.metrics.AvgLatency == 0 {
			p.metrics.AvgLatency = duration
		} else {
			p.metrics.AvgLatency = (p.metrics.AvgLatency + duration) / 2
		}

		if duration > p.metrics.MaxLatency {
			p.metrics.MaxLatency = duration
		}
	}

	p.metrics.CircuitBreakerState = p.circuitBreaker.State().String()
}

// updateMetricsOnError updates metrics after failed message delivery
func (p *KafkaProducer) updateMetricsOnError() {
	p.metricsLock.Lock()
	defer p.metricsLock.Unlock()

	atomic.AddInt64(&p.metrics.MessagesFailedTotal, 1)
	atomic.AddInt64(&p.metrics.MessagesFailedLast, 1)
	p.metrics.CircuitBreakerState = p.circuitBreaker.State().String()
}

// updateHealthOnSuccess updates health status after successful operation
func (p *KafkaProducer) updateHealthOnSuccess() {
	p.healthLock.Lock()
	defer p.healthLock.Unlock()

	p.health.LastSuccess = time.Now()
	p.health.MessagesSent++

	// Determine status based on error rate
	if p.health.ErrorCount == 0 {
		p.health.Status = "healthy"
	} else {
		errorRate := float64(p.health.ErrorCount) / float64(p.health.MessagesSent)
		if errorRate < 0.01 { // Less than 1% error rate
			p.health.Status = "healthy"
		} else if errorRate < 0.05 { // Less than 5% error rate
			p.health.Status = "degraded"
		} else {
			p.health.Status = "unhealthy"
		}
	}
}

// updateHealthOnError updates health status after failed operation
func (p *KafkaProducer) updateHealthOnError(err error) {
	p.healthLock.Lock()
	defer p.healthLock.Unlock()

	p.health.ErrorCount++
	p.health.LastError = err.Error()

	// Determine status
	errorRate := float64(p.health.ErrorCount) / float64(p.health.MessagesSent+p.health.ErrorCount)
	if errorRate < 0.01 {
		p.health.Status = "healthy"
	} else if errorRate < 0.05 {
		p.health.Status = "degraded"
	} else {
		p.health.Status = "unhealthy"
	}
}