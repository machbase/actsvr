package k2m

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds runtime metrics for the K2M broker
type Metrics struct {
	// Message counters
	MessagesReceived  int64 `json:"messagesReceived"`
	MessagesProcessed int64 `json:"messagesProcessed"`
	MessagesPublished int64 `json:"messagesPublished"`
	MessagesFailed    int64 `json:"messagesFailed"`
	MessagesDropped   int64 `json:"messagesDropped"`

	// Error counters
	KafkaErrors     int64 `json:"kafkaErrors"`
	MQTTErrors      int64 `json:"mqttErrors"`
	TransformErrors int64 `json:"transformErrors"`

	// Throughput metrics (messages per second)
	ReceiveRate float64 `json:"receiveRate"`
	ProcessRate float64 `json:"processRate"`
	PublishRate float64 `json:"publishRate"`

	// Latency metrics (in microseconds)
	ProcessingLatency int64 `json:"processingLatencyMicros"`
	PublishLatency    int64 `json:"publishLatencyMicros"`

	// Connection status
	KafkaConnected bool `json:"kafkaConnected"`
	MQTTConnected  bool `json:"mqttConnected"`

	// Worker status
	ActiveWorkers     int     `json:"activeWorkers"`
	BufferUtilization float64 `json:"bufferUtilization"`

	// Timing
	StartTime       time.Time `json:"startTime"`
	LastMessageTime time.Time `json:"lastMessageTime"`

	// Internal tracking
	lastReceived   int64
	lastProcessed  int64
	lastPublished  int64
	lastUpdateTime time.Time
	mu             sync.RWMutex
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	now := time.Now()
	return &Metrics{
		StartTime:      now,
		lastUpdateTime: now,
	}
}

// IncrementMessagesReceived atomically increments the received message counter
func (m *Metrics) IncrementMessagesReceived() {
	atomic.AddInt64(&m.MessagesReceived, 1)
	m.mu.Lock()
	m.LastMessageTime = time.Now()
	m.mu.Unlock()
}

// IncrementMessagesProcessed atomically increments the processed message counter
func (m *Metrics) IncrementMessagesProcessed() {
	atomic.AddInt64(&m.MessagesProcessed, 1)
}

// IncrementMessagesPublished atomically increments the published message counter
func (m *Metrics) IncrementMessagesPublished() {
	atomic.AddInt64(&m.MessagesPublished, 1)
}

// IncrementMessagesFailed atomically increments the failed message counter
func (m *Metrics) IncrementMessagesFailed() {
	atomic.AddInt64(&m.MessagesFailed, 1)
}

// IncrementMessagesDropped atomically increments the dropped message counter
func (m *Metrics) IncrementMessagesDropped() {
	atomic.AddInt64(&m.MessagesDropped, 1)
}

// IncrementKafkaErrors atomically increments the Kafka error counter
func (m *Metrics) IncrementKafkaErrors() {
	atomic.AddInt64(&m.KafkaErrors, 1)
}

// IncrementMQTTErrors atomically increments the MQTT error counter
func (m *Metrics) IncrementMQTTErrors() {
	atomic.AddInt64(&m.MQTTErrors, 1)
}

// IncrementTransformErrors atomically increments the transform error counter
func (m *Metrics) IncrementTransformErrors() {
	atomic.AddInt64(&m.TransformErrors, 1)
}

// IncrementPublishTimeouts atomically increments the publish timeout counter
func (m *Metrics) IncrementPublishTimeouts() {
	atomic.AddInt64(&m.MessagesFailed, 1) // Track as failed messages
}

// RecordProcessingLatency records the processing latency in microseconds
func (m *Metrics) RecordProcessingLatency(duration time.Duration) {
	atomic.StoreInt64(&m.ProcessingLatency, duration.Microseconds())
}

// RecordPublishLatency records the publish latency in microseconds
func (m *Metrics) RecordPublishLatency(duration time.Duration) {
	atomic.StoreInt64(&m.PublishLatency, duration.Microseconds())
}

// SetKafkaConnected sets the Kafka connection status
func (m *Metrics) SetKafkaConnected(connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.KafkaConnected = connected
}

// SetMQTTConnected sets the MQTT connection status
func (m *Metrics) SetMQTTConnected(connected bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MQTTConnected = connected
}

// SetActiveWorkers sets the number of active workers
func (m *Metrics) SetActiveWorkers(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ActiveWorkers = count
}

// SetBufferUtilization sets the buffer utilization percentage (0.0 to 1.0)
func (m *Metrics) SetBufferUtilization(utilization float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.BufferUtilization = utilization
}

// UpdateRates calculates and updates the throughput rates
func (m *Metrics) UpdateRates() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(m.lastUpdateTime).Seconds()
	if elapsed < 1.0 {
		return // Update at most once per second
	}

	currentReceived := atomic.LoadInt64(&m.MessagesReceived)
	currentProcessed := atomic.LoadInt64(&m.MessagesProcessed)
	currentPublished := atomic.LoadInt64(&m.MessagesPublished)

	if elapsed > 0 {
		m.ReceiveRate = float64(currentReceived-m.lastReceived) / elapsed
		m.ProcessRate = float64(currentProcessed-m.lastProcessed) / elapsed
		m.PublishRate = float64(currentPublished-m.lastPublished) / elapsed
	}

	m.lastReceived = currentReceived
	m.lastProcessed = currentProcessed
	m.lastPublished = currentPublished
	m.lastUpdateTime = now
}

// GetSnapshot returns a thread-safe snapshot of the current metrics
func (m *Metrics) GetSnapshot() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Metrics{
		MessagesReceived:  atomic.LoadInt64(&m.MessagesReceived),
		MessagesProcessed: atomic.LoadInt64(&m.MessagesProcessed),
		MessagesPublished: atomic.LoadInt64(&m.MessagesPublished),
		MessagesFailed:    atomic.LoadInt64(&m.MessagesFailed),
		MessagesDropped:   atomic.LoadInt64(&m.MessagesDropped),
		KafkaErrors:       atomic.LoadInt64(&m.KafkaErrors),
		MQTTErrors:        atomic.LoadInt64(&m.MQTTErrors),
		TransformErrors:   atomic.LoadInt64(&m.TransformErrors),
		ReceiveRate:       m.ReceiveRate,
		ProcessRate:       m.ProcessRate,
		PublishRate:       m.PublishRate,
		ProcessingLatency: atomic.LoadInt64(&m.ProcessingLatency),
		PublishLatency:    atomic.LoadInt64(&m.PublishLatency),
		KafkaConnected:    m.KafkaConnected,
		MQTTConnected:     m.MQTTConnected,
		ActiveWorkers:     m.ActiveWorkers,
		BufferUtilization: m.BufferUtilization,
		StartTime:         m.StartTime,
		LastMessageTime:   m.LastMessageTime,
	}
}

// Reset resets all counters and rates
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	atomic.StoreInt64(&m.MessagesReceived, 0)
	atomic.StoreInt64(&m.MessagesProcessed, 0)
	atomic.StoreInt64(&m.MessagesPublished, 0)
	atomic.StoreInt64(&m.MessagesFailed, 0)
	atomic.StoreInt64(&m.MessagesDropped, 0)
	atomic.StoreInt64(&m.KafkaErrors, 0)
	atomic.StoreInt64(&m.MQTTErrors, 0)
	atomic.StoreInt64(&m.TransformErrors, 0)
	atomic.StoreInt64(&m.ProcessingLatency, 0)
	atomic.StoreInt64(&m.PublishLatency, 0)

	m.ReceiveRate = 0
	m.ProcessRate = 0
	m.PublishRate = 0
	m.BufferUtilization = 0

	now := time.Now()
	m.StartTime = now
	m.LastMessageTime = time.Time{}
	m.lastUpdateTime = now
	m.lastReceived = 0
	m.lastProcessed = 0
	m.lastPublished = 0
}

// Uptime returns the duration since the metrics were started
func (m *Metrics) Uptime() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return time.Since(m.StartTime)
}

// IsHealthy returns true if the broker appears to be healthy
func (m *Metrics) IsHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Basic health check: both connections should be active
	return m.KafkaConnected && m.MQTTConnected
}

// GetErrorRate returns the error rate as a percentage of total messages
func (m *Metrics) GetErrorRate() float64 {
	total := atomic.LoadInt64(&m.MessagesReceived)
	if total == 0 {
		return 0.0
	}

	errors := atomic.LoadInt64(&m.MessagesFailed) +
		atomic.LoadInt64(&m.KafkaErrors) +
		atomic.LoadInt64(&m.MQTTErrors) +
		atomic.LoadInt64(&m.TransformErrors)

	return float64(errors) / float64(total) * 100.0
}
