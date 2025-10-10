package k2m

import (
	"actsvr/util"
	"encoding/json"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.NotNil(t, config)
	assert.Equal(t, []string{"localhost:9092"}, config.KafkaConfig.Brokers)
	assert.Equal(t, []string{"test-topic"}, config.KafkaConfig.Topics)
	assert.Equal(t, "k2m-consumer-group", config.KafkaConfig.ConsumerGroup)
	assert.True(t, config.KafkaConfig.ReturnErrors)
	assert.True(t, config.KafkaConfig.OffsetOldest)
	assert.Equal(t, 10*time.Second, config.KafkaConfig.SessionTimeout)
	assert.Equal(t, 3*time.Second, config.KafkaConfig.HeartbeatInterval)

	assert.Equal(t, "tcp://localhost:1883", config.MQTTConfig.Broker)
	assert.Equal(t, "k2m-broker", config.MQTTConfig.ClientID)
	assert.Equal(t, byte(1), config.MQTTConfig.QoS)
	assert.False(t, config.MQTTConfig.Retained)
	assert.Equal(t, 60*time.Second, config.MQTTConfig.KeepAlive)
	assert.Equal(t, 1*time.Second, config.MQTTConfig.PingTimeout)
	assert.True(t, config.MQTTConfig.ConnectRetry)
	assert.Equal(t, 10*time.Minute, config.MQTTConfig.MaxReconnectInterval)

	assert.Len(t, config.Routes, 1)
	assert.Equal(t, "default", config.Routes[0].Name)
	assert.Equal(t, "{kafkaTopic}", config.Routes[0].Mapping.KafkaTopic)
	assert.Equal(t, "mqtt/{kafkaTopic}", config.Routes[0].Mapping.MQTTTopic)
	assert.Equal(t, "none", config.Routes[0].Mapping.Transform)

	assert.Equal(t, 5, config.WorkerCount)
	assert.Equal(t, 1000, config.BufferSize)
}

func TestK2MConfigJSONSerialization(t *testing.T) {
	originalConfig := DefaultConfig()
	originalConfig.KafkaConfig.Brokers = []string{"broker1:9092", "broker2:9092"}
	originalConfig.MQTTConfig.Username = "testuser"
	originalConfig.MQTTConfig.Password = "testpass"

	// Marshal to JSON
	jsonData, err := json.Marshal(originalConfig)
	require.NoError(t, err)

	// Unmarshal from JSON
	var deserializedConfig K2MConfig
	err = json.Unmarshal(jsonData, &deserializedConfig)
	require.NoError(t, err)

	// Verify deserialized config matches original
	assert.Equal(t, originalConfig.KafkaConfig.Brokers, deserializedConfig.KafkaConfig.Brokers)
	assert.Equal(t, originalConfig.MQTTConfig.Username, deserializedConfig.MQTTConfig.Username)
	assert.Equal(t, originalConfig.MQTTConfig.Password, deserializedConfig.MQTTConfig.Password)
	assert.Equal(t, originalConfig.WorkerCount, deserializedConfig.WorkerCount)
	assert.Equal(t, originalConfig.BufferSize, deserializedConfig.BufferSize)
}

func TestNewK2MBroker(t *testing.T) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()

	// Test successful creation
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)
	assert.NotNil(t, broker)
	assert.Equal(t, config, broker.config)
	assert.Equal(t, logger, broker.logger)
	assert.NotNil(t, broker.ctx)
	assert.NotNil(t, broker.cancel)
	assert.NotNil(t, broker.ready)
	assert.NotNil(t, broker.messageCh)

	// Test with nil config (should use default)
	broker2, err := NewK2MBroker(nil, logger)
	require.NoError(t, err)
	assert.NotNil(t, broker2)
	assert.NotNil(t, broker2.config)

	// Test with nil logger (should fail)
	broker3, err := NewK2MBroker(config, nil)
	assert.Error(t, err)
	assert.Nil(t, broker3)
	assert.Contains(t, err.Error(), "logger cannot be nil")
}

func TestK2MBrokerContext(t *testing.T) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()

	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	// Test context cancellation
	select {
	case <-broker.ctx.Done():
		t.Fatal("Context should not be cancelled initially")
	default:
		// Expected
	}

	// Cancel context
	broker.cancel()

	select {
	case <-broker.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Context should be cancelled after calling cancel()")
	}
}

func TestTopicMapping(t *testing.T) {
	tests := []struct {
		name     string
		mapping  TopicMapping
		expected TopicMapping
	}{
		{
			name: "Basic mapping",
			mapping: TopicMapping{
				KafkaTopic: "sensor-data",
				MQTTTopic:  "iot/sensors",
				Transform:  "none",
			},
			expected: TopicMapping{
				KafkaTopic: "sensor-data",
				MQTTTopic:  "iot/sensors",
				Transform:  "none",
			},
		},
		{
			name: "JSON transform mapping",
			mapping: TopicMapping{
				KafkaTopic: "user-events",
				MQTTTopic:  "events/users",
				Transform:  "json",
			},
			expected: TopicMapping{
				KafkaTopic: "user-events",
				MQTTTopic:  "events/users",
				Transform:  "json",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected.KafkaTopic, tt.mapping.KafkaTopic)
			assert.Equal(t, tt.expected.MQTTTopic, tt.mapping.MQTTTopic)
			assert.Equal(t, tt.expected.Transform, tt.mapping.Transform)
		})
	}
}

func TestMessageWorkerTransformMessage(t *testing.T) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	worker := &MessageWorker{
		id:     1,
		broker: broker,
	}

	// Create test Kafka message
	kafkaMsg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       []byte("test-key"),
		Value:     []byte("test-payload"),
		Timestamp: time.Now(),
	}

	t.Run("Transform none", func(t *testing.T) {
		mapping := &TopicMapping{Transform: "none"}
		result, err := worker.transformMessage(kafkaMsg, mapping)
		require.NoError(t, err)
		assert.Equal(t, []byte("test-payload"), result)
	})

	t.Run("Transform json", func(t *testing.T) {
		mapping := &TopicMapping{Transform: "json"}
		result, err := worker.transformMessage(kafkaMsg, mapping)
		require.NoError(t, err)

		var envelope map[string]interface{}
		err = json.Unmarshal(result, &envelope)
		require.NoError(t, err)

		assert.Equal(t, "test-topic", envelope["kafkaTopic"])
		assert.Equal(t, float64(0), envelope["kafkaPartition"]) // JSON numbers are float64
		assert.Equal(t, float64(123), envelope["kafkaOffset"])
		assert.Equal(t, "test-key", envelope["key"])
		assert.Equal(t, "test-payload", envelope["value"])
		assert.Contains(t, envelope, "timestamp")
	})

	t.Run("Transform unknown defaults to none", func(t *testing.T) {
		mapping := &TopicMapping{Transform: "unknown"}
		result, err := worker.transformMessage(kafkaMsg, mapping)
		require.NoError(t, err)
		assert.Equal(t, []byte("test-payload"), result)
	})
}

func TestMessageWorkerResolveMQTTTopic(t *testing.T) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	worker := &MessageWorker{
		id:     1,
		broker: broker,
	}

	// Create test Kafka message
	kafkaMsg := &sarama.ConsumerMessage{
		Topic:     "sensor-data",
		Partition: 2,
		Key:       []byte("device-123"),
		Value:     []byte("test-payload"),
	}

	tests := []struct {
		name     string
		template string
		expected string
	}{
		{
			name:     "Static topic",
			template: "mqtt/static",
			expected: "mqtt/static",
		},
		{
			name:     "Kafka topic template",
			template: "mqtt/{kafkaTopic}",
			expected: "mqtt/sensor-data",
		},
		{
			name:     "Partition template",
			template: "mqtt/partition-{partition}",
			expected: "mqtt/partition-2",
		},
		{
			name:     "Key template",
			template: "mqtt/devices/{key}",
			expected: "mqtt/devices/device-123",
		},
		{
			name:     "Multiple templates",
			template: "mqtt/{kafkaTopic}/partition-{partition}/device-{key}",
			expected: "mqtt/sensor-data/partition-2/device-device-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := worker.resolveMQTTTopic(tt.template, kafkaMsg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestKafkaConfig(t *testing.T) {
	config := KafkaConfig{
		Brokers:           []string{"broker1:9092", "broker2:9092"},
		Topics:            []string{"topic1", "topic2"},
		ConsumerGroup:     "test-group",
		ReturnErrors:      true,
		OffsetOldest:      false,
		SessionTimeout:    Duration(15 * time.Second),
		HeartbeatInterval: Duration(5 * time.Second),
	}

	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, config.Brokers)
	assert.Equal(t, []string{"topic1", "topic2"}, config.Topics)
	assert.Equal(t, "test-group", config.ConsumerGroup)
	assert.True(t, config.ReturnErrors)
	assert.False(t, config.OffsetOldest)
	assert.Equal(t, 15*time.Second, config.SessionTimeout)
	assert.Equal(t, 5*time.Second, config.HeartbeatInterval)
}

func TestMQTTConfig(t *testing.T) {
	config := MQTTConfig{
		Broker:               "tcp://mqtt.example.com:1883",
		ClientID:             "test-client",
		Username:             "user",
		Password:             "pass",
		QoS:                  2,
		Retained:             true,
		KeepAlive:            Duration(30 * time.Second),
		PingTimeout:          Duration(5 * time.Second),
		ConnectRetry:         false,
		MaxReconnectInterval: Duration(5 * time.Minute),
	}

	assert.Equal(t, "tcp://mqtt.example.com:1883", config.Broker)
	assert.Equal(t, "test-client", config.ClientID)
	assert.Equal(t, "user", config.Username)
	assert.Equal(t, "pass", config.Password)
	assert.Equal(t, byte(2), config.QoS)
	assert.True(t, config.Retained)
	assert.Equal(t, 30*time.Second, config.KeepAlive)
	assert.Equal(t, 5*time.Second, config.PingTimeout)
	assert.False(t, config.ConnectRetry)
	assert.Equal(t, 5*time.Minute, config.MaxReconnectInterval)
}

func BenchmarkTransformMessageNone(b *testing.B) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()
	broker, _ := NewK2MBroker(config, logger)

	worker := &MessageWorker{
		id:     1,
		broker: broker,
	}

	kafkaMsg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       []byte("test-key"),
		Value:     []byte("test-payload"),
		Timestamp: time.Now(),
	}

	mapping := &TopicMapping{Transform: "none"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = worker.transformMessage(kafkaMsg, mapping)
	}
}

func BenchmarkTransformMessageJSON(b *testing.B) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()
	broker, _ := NewK2MBroker(config, logger)

	worker := &MessageWorker{
		id:     1,
		broker: broker,
	}

	kafkaMsg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       []byte("test-key"),
		Value:     []byte("test-payload"),
		Timestamp: time.Now(),
	}

	mapping := &TopicMapping{Transform: "json"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = worker.transformMessage(kafkaMsg, mapping)
	}
}

func BenchmarkResolveMQTTTopic(b *testing.B) {
	logger := util.NewLog(util.DefaultLogConfig())
	config := DefaultConfig()
	broker, _ := NewK2MBroker(config, logger)

	worker := &MessageWorker{
		id:     1,
		broker: broker,
	}

	kafkaMsg := &sarama.ConsumerMessage{
		Topic:     "sensor-data",
		Partition: 2,
		Key:       []byte("device-123"),
		Value:     []byte("test-payload"),
	}

	template := "mqtt/{kafkaTopic}/partition-{partition}/device-{key}"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = worker.resolveMQTTTopic(template, kafkaMsg)
	}
}
