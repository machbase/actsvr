package k2m

import (
	"actsvr/util"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockMQTTClient implements a mock MQTT client for testing
type MockMQTTClient struct {
	mock.Mock
	connected bool
	messages  []MockMessage
	mu        sync.RWMutex
}

type MockMessage struct {
	Topic    string
	QoS      byte
	Retained bool
	Payload  []byte
}

func NewMockMQTTClient() *MockMQTTClient {
	return &MockMQTTClient{
		connected: false,
		messages:  make([]MockMessage, 0),
	}
}

func (m *MockMQTTClient) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

func (m *MockMQTTClient) IsConnectionOpen() bool {
	return m.IsConnected()
}

func (m *MockMQTTClient) Connect() mqtt.Token {
	token := &MockToken{}
	m.mu.Lock()
	m.connected = true
	m.mu.Unlock()
	return token
}

func (m *MockMQTTClient) Disconnect(quiesce uint) {
	m.mu.Lock()
	m.connected = false
	m.mu.Unlock()
}

func (m *MockMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	m.mu.Lock()
	defer m.mu.Unlock()

	var payloadBytes []byte
	switch p := payload.(type) {
	case []byte:
		payloadBytes = p
	case string:
		payloadBytes = []byte(p)
	default:
		payloadBytes = []byte(fmt.Sprintf("%v", p))
	}

	m.messages = append(m.messages, MockMessage{
		Topic:    topic,
		QoS:      qos,
		Retained: retained,
		Payload:  payloadBytes,
	})

	return &MockToken{}
}

func (m *MockMQTTClient) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	return &MockToken{}
}

func (m *MockMQTTClient) SubscribeMultiple(filters map[string]byte, callback mqtt.MessageHandler) mqtt.Token {
	return &MockToken{}
}

func (m *MockMQTTClient) Unsubscribe(topics ...string) mqtt.Token {
	return &MockToken{}
}

func (m *MockMQTTClient) AddRoute(topic string, callback mqtt.MessageHandler) {
}

func (m *MockMQTTClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.ClientOptionsReader{}
}

func (m *MockMQTTClient) GetMessages() []MockMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]MockMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

func (m *MockMQTTClient) ClearMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = m.messages[:0]
}

// MockToken implements a mock MQTT token for testing
type MockToken struct {
	mock.Mock
	err error
}

func (m *MockToken) Wait() bool {
	return true
}

func (m *MockToken) WaitTimeout(time.Duration) bool {
	return true
}

func (m *MockToken) Error() error {
	return m.err
}

func (m *MockToken) SetError(err error) {
	m.err = err
}

func (m *MockToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// MockSaramaConsumerGroup implements a mock Kafka consumer group for testing
type MockSaramaConsumerGroup struct {
	mock.Mock
	errorsCh chan error
	closed   bool
	mu       sync.RWMutex
}

func NewMockSaramaConsumerGroup() *MockSaramaConsumerGroup {
	return &MockSaramaConsumerGroup{
		errorsCh: make(chan error, 10),
	}
}

func (m *MockSaramaConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	args := m.Called(ctx, topics, handler)

	// Simulate consumer group session
	if !args.Bool(0) { // If should fail
		return fmt.Errorf("mock consume error")
	}

	// Setup mock session and claim
	session := &MockConsumerGroupSession{}
	claim := &MockConsumerGroupClaim{
		messagesCh: make(chan *sarama.ConsumerMessage, 10),
		topic:      "test-topic",
		partition:  0,
	}

	// Call handler methods
	if err := handler.Setup(session); err != nil {
		return err
	}

	// Start consuming in goroutine
	go func() {
		defer handler.Cleanup(session)
		handler.ConsumeClaim(session, claim)
	}()

	// Keep consuming until context is done
	<-ctx.Done()
	return nil
}

func (m *MockSaramaConsumerGroup) Errors() <-chan error {
	return m.errorsCh
}

func (m *MockSaramaConsumerGroup) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		close(m.errorsCh)
		m.closed = true
	}
	return nil
}

func (m *MockSaramaConsumerGroup) Pause(partitions map[string][]int32) {
	// Mock implementation - do nothing
}

func (m *MockSaramaConsumerGroup) Resume(partitions map[string][]int32) {
	// Mock implementation - do nothing
}

func (m *MockSaramaConsumerGroup) PauseAll() {
	// Mock implementation - do nothing
}

func (m *MockSaramaConsumerGroup) ResumeAll() {
	// Mock implementation - do nothing
}

// MockConsumerGroupSession implements a mock consumer group session
type MockConsumerGroupSession struct {
	mock.Mock
}

func (m *MockConsumerGroupSession) Claims() map[string][]int32 {
	return map[string][]int32{"test-topic": {0}}
}

func (m *MockConsumerGroupSession) MemberID() string {
	return "mock-member"
}

func (m *MockConsumerGroupSession) GenerationID() int32 {
	return 1
}

func (m *MockConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
}

func (m *MockConsumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
}

func (m *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
}

func (m *MockConsumerGroupSession) Context() context.Context {
	return context.Background()
}

func (m *MockConsumerGroupSession) Commit() {
}

// MockConsumerGroupClaim implements a mock consumer group claim
type MockConsumerGroupClaim struct {
	messagesCh chan *sarama.ConsumerMessage
	topic      string
	partition  int32
}

func (m *MockConsumerGroupClaim) Topic() string {
	return m.topic
}

func (m *MockConsumerGroupClaim) Partition() int32 {
	return m.partition
}

func (m *MockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

func (m *MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 100
}

func (m *MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.messagesCh
}

func (m *MockConsumerGroupClaim) AddMessage(msg *sarama.ConsumerMessage) {
	select {
	case m.messagesCh <- msg:
	default:
		// Channel full, ignore
	}
}

func (m *MockConsumerGroupClaim) Close() {
	close(m.messagesCh)
}

// Test broker initialization and basic functionality
func TestK2MBrokerInitialization(t *testing.T) {
	logger := &util.Log{}
	config := DefaultConfig()

	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)
	assert.NotNil(t, broker)

	// Test initial state
	assert.Equal(t, config, broker.config)
	assert.Equal(t, logger, broker.logger)
	assert.NotNil(t, broker.ctx)
	assert.NotNil(t, broker.cancel)
	assert.NotNil(t, broker.ready)
	assert.NotNil(t, broker.messageCh)
	assert.Equal(t, config.BufferSize, cap(broker.messageCh))
}

func TestK2MBrokerWithMockClients(t *testing.T) {
	logger := &util.Log{}
	config := DefaultConfig()
	config.WorkerCount = 2
	config.BufferSize = 10

	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	// Replace with mock clients
	mockMQTT := NewMockMQTTClient()
	mockKafka := NewMockSaramaConsumerGroup()

	broker.mqttClient = mockMQTT
	broker.consumerGroup = mockKafka

	// Set up mock expectations
	mockKafka.On("Consume", mock.AnythingOfType("*context.cancelCtx"), config.KafkaConfig.Topics, mock.AnythingOfType("*k2m.Consumer")).Return(true)

	// Test message processing
	testMessage := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       []byte("test-key"),
		Value:     []byte("test-message"),
		Timestamp: time.Now(),
	}

	// Create a consumer for testing
	consumer := &Consumer{
		ready:  make(chan bool),
		broker: broker,
	}
	broker.consumer = consumer

	// Test consumer setup
	session := &MockConsumerGroupSession{}
	err = consumer.Setup(session)
	assert.NoError(t, err)

	// Verify ready channel is closed
	select {
	case <-consumer.ready:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Ready channel should be closed after Setup")
	}

	// Test message worker transformation
	worker := &MessageWorker{
		id:        1,
		broker:    broker,
		messageCh: broker.messageCh,
	}

	// Test message processing - use specific route for test-topic
	broker.config.Routes = []RouteConfig{
		{
			Name:     "test-route",
			Priority: 10,
			Filters: []FilterConfig{
				{
					Type: "topic",
					Config: map[string]interface{}{
						"pattern": "test-topic",
					},
				},
			},
			Mapping: TopicMapping{
				KafkaTopic: "test-topic",
				MQTTTopic:  "mqtt/test",
				Transform:  "none",
			},
		},
	}

	// Recreate router with the test config
	router, err := NewMessageRouter(broker.config.Routes)
	require.NoError(t, err)
	broker.router = router

	// Process the message
	worker.processMessage(testMessage)

	// Verify MQTT message was published
	messages := mockMQTT.GetMessages()
	require.Len(t, messages, 1)
	assert.Equal(t, "mqtt/test", messages[0].Topic)
	assert.Equal(t, []byte("test-message"), messages[0].Payload)
	assert.Equal(t, byte(1), messages[0].QoS)
	assert.False(t, messages[0].Retained)
}

func TestMessageWorkerProcessMessage(t *testing.T) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	mockMQTT := NewMockMQTTClient()
	broker.mqttClient = mockMQTT

	worker := &MessageWorker{
		id:        1,
		broker:    broker,
		messageCh: broker.messageCh,
	}

	// Test message with no matching route (should match default route)
	testMessage := &sarama.ConsumerMessage{
		Topic:     "unknown-topic",
		Partition: 0,
		Offset:    123,
		Key:       []byte("test-key"),
		Value:     []byte("test-message"),
		Timestamp: time.Now(),
	}

	// Process message (should match default route)
	worker.processMessage(testMessage)
	messages := mockMQTT.GetMessages()
	require.Len(t, messages, 1)
	assert.Equal(t, "mqtt/unknown-topic", messages[0].Topic) // Default route uses mqtt/{kafkaTopic} pattern

	// Clear messages and add specific route
	mockMQTT.ClearMessages()
	broker.config.Routes = []RouteConfig{
		{
			Name:     "unknown-route",
			Priority: 10,
			Filters: []FilterConfig{
				{
					Type: "topic",
					Config: map[string]interface{}{
						"pattern": "unknown-topic",
					},
				},
			},
			Mapping: TopicMapping{
				KafkaTopic: "unknown-topic",
				MQTTTopic:  "mqtt/unknown",
				Transform:  "json",
			},
		},
	}

	// Recreate router with the specific route
	router, err := NewMessageRouter(broker.config.Routes)
	require.NoError(t, err)
	broker.router = router

	// Process message with specific route (should publish with JSON transform)
	worker.processMessage(testMessage)
	messages = mockMQTT.GetMessages()
	require.Len(t, messages, 1)

	// Verify JSON transformation
	assert.Equal(t, "mqtt/unknown", messages[0].Topic)
	assert.Contains(t, string(messages[0].Payload), "kafkaTopic")
	assert.Contains(t, string(messages[0].Payload), "unknown-topic")
}

func TestConsumerCleanup(t *testing.T) {
	logger := &util.Log{}
	config := DefaultConfig()
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	consumer := &Consumer{
		ready:  make(chan bool),
		broker: broker,
	}

	session := &MockConsumerGroupSession{}
	err = consumer.Cleanup(session)
	assert.NoError(t, err)
}

func TestBrokerStop(t *testing.T) {
	logger := &util.Log{}
	config := DefaultConfig()
	config.WorkerCount = 1

	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	mockMQTT := NewMockMQTTClient()
	mockMQTT.connected = true
	mockKafka := NewMockSaramaConsumerGroup()

	broker.mqttClient = mockMQTT
	broker.consumerGroup = mockKafka

	// Test stop
	err = broker.Stop()
	assert.NoError(t, err)

	// Verify context is cancelled
	select {
	case <-broker.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Context should be cancelled after Stop")
	}

	// Verify MQTT client is disconnected
	assert.False(t, mockMQTT.IsConnected())
}

func TestTopicMappingEdgeCases(t *testing.T) {
	logger := &util.Log{}
	config := DefaultConfig()
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	worker := &MessageWorker{
		id:     1,
		broker: broker,
	}

	// Test with empty key
	kafkaMsg := &sarama.ConsumerMessage{
		Topic:     "sensor-data",
		Partition: 2,
		Key:       []byte(""),
		Value:     []byte("test-payload"),
	}

	result := worker.resolveMQTTTopic("mqtt/devices/{key}", kafkaMsg)
	assert.Equal(t, "mqtt/devices/", result)

	// Test with nil key
	kafkaMsg.Key = nil
	result = worker.resolveMQTTTopic("mqtt/devices/{key}", kafkaMsg)
	assert.Equal(t, "mqtt/devices/", result)
}

// Integration test simulating a complete message flow
func TestMessageFlowIntegration(t *testing.T) {
	logger := &util.Log{}
	config := DefaultConfig()
	config.WorkerCount = 1
	config.BufferSize = 5
	config.Routes = []RouteConfig{
		{
			Name:     "sensor-route",
			Priority: 10,
			Filters: []FilterConfig{
				{
					Type: "topic",
					Config: map[string]interface{}{
						"pattern": "sensor-data",
					},
				},
			},
			Mapping: TopicMapping{
				KafkaTopic: "sensor-data",
				MQTTTopic:  "iot/sensors/{key}",
				Transform:  "json",
			},
		},
	}

	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	mockMQTT := NewMockMQTTClient()
	broker.mqttClient = mockMQTT

	// Create consumer and worker
	consumer := &Consumer{
		ready:  make(chan bool),
		broker: broker,
	}
	broker.consumer = consumer

	worker := &MessageWorker{
		id:        1,
		broker:    broker,
		messageCh: broker.messageCh,
	}

	// Start worker in goroutine
	ctx, cancel := context.WithCancel(context.Background())
	broker.ctx = ctx
	broker.cancel = cancel

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case message, ok := <-worker.messageCh:
				if !ok {
					return
				}
				worker.processMessage(message)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Send test messages
	testMessages := []*sarama.ConsumerMessage{
		{
			Topic:     "sensor-data",
			Partition: 0,
			Offset:    1,
			Key:       []byte("device-001"),
			Value:     []byte(`{"temperature": 25.5, "humidity": 60}`),
			Timestamp: time.Now(),
		},
		{
			Topic:     "sensor-data",
			Partition: 0,
			Offset:    2,
			Key:       []byte("device-002"),
			Value:     []byte(`{"temperature": 22.1, "humidity": 55}`),
			Timestamp: time.Now(),
		},
	}

	for _, msg := range testMessages {
		select {
		case broker.messageCh <- msg:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Failed to send message to worker")
		}
	}

	// Wait for processing
	time.Sleep(50 * time.Millisecond)

	// Stop broker
	cancel()
	close(broker.messageCh)
	wg.Wait()

	// Verify messages were published
	messages := mockMQTT.GetMessages()
	require.Len(t, messages, 2)

	// Verify first message
	assert.Equal(t, "iot/sensors/device-001", messages[0].Topic)
	assert.Contains(t, string(messages[0].Payload), "kafkaTopic")
	assert.Contains(t, string(messages[0].Payload), "sensor-data")
	assert.Contains(t, string(messages[0].Payload), "device-001")

	// Verify second message
	assert.Equal(t, "iot/sensors/device-002", messages[1].Topic)
	assert.Contains(t, string(messages[1].Payload), "device-002")
}
