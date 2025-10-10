package main

import (
	"actsvr/k2m"
	"encoding/json"
	"flag"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTopicMappings(t *testing.T) {
	tests := []struct {
		name        string
		mappingsStr string
		transform   string
		expected    []k2m.TopicMapping
	}{
		{
			name:        "Single mapping",
			mappingsStr: "kafka-topic=mqtt-topic",
			transform:   "none",
			expected: []k2m.TopicMapping{
				{
					KafkaTopic: "kafka-topic",
					MQTTTopic:  "mqtt-topic",
					Transform:  "none",
				},
			},
		},
		{
			name:        "Multiple mappings",
			mappingsStr: "topic1=mqtt1,topic2=mqtt2",
			transform:   "json",
			expected: []k2m.TopicMapping{
				{
					KafkaTopic: "topic1",
					MQTTTopic:  "mqtt1",
					Transform:  "json",
				},
				{
					KafkaTopic: "topic2",
					MQTTTopic:  "mqtt2",
					Transform:  "json",
				},
			},
		},
		{
			name:        "Mappings with spaces",
			mappingsStr: "topic 1 = mqtt 1 , topic 2 = mqtt 2",
			transform:   "none",
			expected: []k2m.TopicMapping{
				{
					KafkaTopic: "topic 1",
					MQTTTopic:  "mqtt 1",
					Transform:  "none",
				},
				{
					KafkaTopic: "topic 2",
					MQTTTopic:  "mqtt 2",
					Transform:  "none",
				},
			},
		},
		{
			name:        "Empty string",
			mappingsStr: "",
			transform:   "none",
			expected:    []k2m.TopicMapping{},
		},
		{
			name:        "Invalid format",
			mappingsStr: "invalid-format",
			transform:   "none",
			expected:    []k2m.TopicMapping{},
		},
		{
			name:        "Mixed valid and invalid",
			mappingsStr: "valid=mqtt,invalid-format,another=valid",
			transform:   "json",
			expected: []k2m.TopicMapping{
				{
					KafkaTopic: "valid",
					MQTTTopic:  "mqtt",
					Transform:  "json",
				},
				{
					KafkaTopic: "another",
					MQTTTopic:  "valid",
					Transform:  "json",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseTopicMappings(tt.mappingsStr, tt.transform)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyCommandLineFlags(t *testing.T) {
	// Save original command line args
	originalArgs := os.Args
	defer func() { os.Args = originalArgs }()

	// Reset flag for testing
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Re-define flags for testing
	var (
		kafkaBrokers     = flag.String("kafka-brokers", "localhost:9092", "Comma-separated list of Kafka brokers")
		kafkaTopics      = flag.String("kafka-topics", "test-topic", "Comma-separated list of Kafka topics to consume")
		consumerGroup    = flag.String("consumer-group", "k2m-consumer-group", "Kafka consumer group ID")
		offsetOldest     = flag.Bool("offset-oldest", true, "Start consuming from the oldest offset")
		mqttBroker       = flag.String("mqtt-broker", "tcp://localhost:1883", "MQTT broker URL")
		mqttClientID     = flag.String("mqtt-client-id", "k2m-broker", "MQTT client ID")
		mqttUsername     = flag.String("mqtt-username", "", "MQTT username")
		mqttPassword     = flag.String("mqtt-password", "", "MQTT password")
		mqttQoS          = flag.Int("mqtt-qos", 1, "MQTT QoS level (0, 1, or 2)")
		mqttRetained     = flag.Bool("mqtt-retained", false, "Publish MQTT messages as retained")
		topicMappings    = flag.String("topic-mappings", "test-topic=mqtt/test", "Comma-separated topic mappings")
		messageTransform = flag.String("message-transform", "none", "Message transformation")
		workerCount      = flag.Int("workers", 5, "Number of message processing workers")
		bufferSize       = flag.Int("buffer-size", 1000, "Message buffer size")
	)

	// Test with custom flags
	os.Args = []string{
		"k2mbroker",
		"-kafka-brokers", "broker1:9092,broker2:9092",
		"-kafka-topics", "topic1,topic2",
		"-consumer-group", "test-group",
		"-offset-oldest=false",
		"-mqtt-broker", "tcp://mqtt.example.com:1883",
		"-mqtt-client-id", "test-client",
		"-mqtt-username", "testuser",
		"-mqtt-password", "testpass",
		"-mqtt-qos", "2",
		"-mqtt-retained=true",
		"-topic-mappings", "topic1=mqtt1,topic2=mqtt2",
		"-message-transform", "json",
		"-workers", "10",
		"-buffer-size", "2000",
	}

	flag.Parse()

	config := k2m.DefaultConfig()

	// Apply flags using a modified version of the function
	applyTestFlags(config, kafkaBrokers, kafkaTopics, consumerGroup, offsetOldest,
		mqttBroker, mqttClientID, mqttUsername, mqttPassword, mqttQoS, mqttRetained,
		topicMappings, messageTransform, workerCount, bufferSize)

	// Verify Kafka configuration
	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, config.KafkaConfig.Brokers)
	assert.Equal(t, []string{"topic1", "topic2"}, config.KafkaConfig.Topics)
	assert.Equal(t, "test-group", config.KafkaConfig.ConsumerGroup)
	assert.False(t, config.KafkaConfig.OffsetOldest)

	// Verify MQTT configuration
	assert.Equal(t, "tcp://mqtt.example.com:1883", config.MQTTConfig.Broker)
	assert.Equal(t, "test-client", config.MQTTConfig.ClientID)
	assert.Equal(t, "testuser", config.MQTTConfig.Username)
	assert.Equal(t, "testpass", config.MQTTConfig.Password)
	assert.Equal(t, byte(2), config.MQTTConfig.QoS)
	assert.True(t, config.MQTTConfig.Retained)

	// Verify topic mappings
	assert.Len(t, config.TopicMappings, 2)
	assert.Equal(t, "topic1", config.TopicMappings[0].KafkaTopic)
	assert.Equal(t, "mqtt1", config.TopicMappings[0].MQTTTopic)
	assert.Equal(t, "json", config.TopicMappings[0].Transform)

	// Verify worker configuration
	assert.Equal(t, 10, config.WorkerCount)
	assert.Equal(t, 2000, config.BufferSize)
}

// Helper function to apply test flags (modified version of applyCommandLineFlags for testing)
func applyTestFlags(config *k2m.K2MConfig, kafkaBrokers, kafkaTopics, consumerGroup *string, offsetOldest *bool,
	mqttBroker, mqttClientID, mqttUsername, mqttPassword *string, mqttQoS *int, mqttRetained *bool,
	topicMappings, messageTransform *string, workerCount, bufferSize *int) {

	// Kafka configuration
	config.KafkaConfig.Brokers = strings.Split(*kafkaBrokers, ",")
	config.KafkaConfig.Topics = strings.Split(*kafkaTopics, ",")
	config.KafkaConfig.ConsumerGroup = *consumerGroup
	config.KafkaConfig.OffsetOldest = *offsetOldest

	// MQTT configuration
	config.MQTTConfig.Broker = *mqttBroker
	config.MQTTConfig.ClientID = *mqttClientID
	if *mqttUsername != "" {
		config.MQTTConfig.Username = *mqttUsername
	}
	if *mqttPassword != "" {
		config.MQTTConfig.Password = *mqttPassword
	}
	config.MQTTConfig.QoS = byte(*mqttQoS)
	config.MQTTConfig.Retained = *mqttRetained

	// Topic mappings
	config.TopicMappings = parseTopicMappings(*topicMappings, *messageTransform)

	// Worker configuration
	config.WorkerCount = *workerCount
	config.BufferSize = *bufferSize
}

func TestLoadConfigurationFromFile(t *testing.T) {
	// Create a temporary config file
	configData := k2m.K2MConfig{
		KafkaConfig: k2m.KafkaConfig{
			Brokers:           []string{"kafka1:9092", "kafka2:9092"},
			Topics:            []string{"sensor-data", "user-events"},
			ConsumerGroup:     "production-group",
			ReturnErrors:      true,
			OffsetOldest:      false,
			SessionTimeout:    k2m.Duration(15 * time.Second),
			HeartbeatInterval: k2m.Duration(5 * time.Second),
		},
		MQTTConfig: k2m.MQTTConfig{
			Broker:               "tcp://mqtt.production.com:1883",
			ClientID:             "production-client",
			Username:             "prod-user",
			Password:             "prod-pass",
			QoS:                  2,
			Retained:             true,
			KeepAlive:            k2m.Duration(120 * time.Second),
			PingTimeout:          k2m.Duration(2 * time.Second),
			ConnectRetry:         true,
			MaxReconnectInterval: k2m.Duration(5 * time.Minute),
		},
		TopicMappings: []k2m.TopicMapping{
			{
				KafkaTopic: "sensor-data",
				MQTTTopic:  "iot/sensors/{key}",
				Transform:  "json",
			},
			{
				KafkaTopic: "user-events",
				MQTTTopic:  "events/users",
				Transform:  "none",
			},
		},
		WorkerCount: 20,
		BufferSize:  5000,
	}

	configJSON, err := json.MarshalIndent(configData, "", "  ")
	require.NoError(t, err)

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "k2m-test-config-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(configJSON)
	require.NoError(t, err)
	tmpFile.Close()

	// Test loading configuration from file
	config, err := loadConfiguration(tmpFile.Name())
	require.NoError(t, err)

	// Verify loaded configuration
	assert.Equal(t, configData.KafkaConfig.Brokers, config.KafkaConfig.Brokers)
	assert.Equal(t, configData.KafkaConfig.Topics, config.KafkaConfig.Topics)
	assert.Equal(t, configData.KafkaConfig.ConsumerGroup, config.KafkaConfig.ConsumerGroup)
	assert.Equal(t, configData.KafkaConfig.OffsetOldest, config.KafkaConfig.OffsetOldest)

	assert.Equal(t, configData.MQTTConfig.Broker, config.MQTTConfig.Broker)
	assert.Equal(t, configData.MQTTConfig.ClientID, config.MQTTConfig.ClientID)
	assert.Equal(t, configData.MQTTConfig.Username, config.MQTTConfig.Username)
	assert.Equal(t, configData.MQTTConfig.Password, config.MQTTConfig.Password)
	assert.Equal(t, configData.MQTTConfig.QoS, config.MQTTConfig.QoS)
	assert.Equal(t, configData.MQTTConfig.Retained, config.MQTTConfig.Retained)

	assert.Len(t, config.TopicMappings, 2)
	assert.Equal(t, configData.WorkerCount, config.WorkerCount)
	assert.Equal(t, configData.BufferSize, config.BufferSize)
}

func TestLoadConfigurationNonExistentFile(t *testing.T) {
	_, err := loadConfiguration("/non/existent/file.json")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read config file")
}

func TestLoadConfigurationInvalidJSON(t *testing.T) {
	// Create temporary file with invalid JSON
	tmpFile, err := os.CreateTemp("", "k2m-test-invalid-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("{ invalid json")
	require.NoError(t, err)
	tmpFile.Close()

	_, err = loadConfiguration(tmpFile.Name())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse config file")
}

func TestWritePIDFile(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "k2m-test-pid-*.pid")
	require.NoError(t, err)
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	err = writePIDFile(tmpFile.Name())
	assert.NoError(t, err)

	// Verify PID file content
	content, err := os.ReadFile(tmpFile.Name())
	require.NoError(t, err)
	assert.Contains(t, string(content), "\n")
	assert.NotEmpty(t, strings.TrimSpace(string(content)))
}

func TestWritePIDFileEmpty(t *testing.T) {
	err := writePIDFile("")
	assert.NoError(t, err)
}

func TestRemovePIDFile(t *testing.T) {
	// Create temporary file
	tmpFile, err := os.CreateTemp("", "k2m-test-pid-*.pid")
	require.NoError(t, err)
	tmpFile.Close()

	// Verify file exists
	_, err = os.Stat(tmpFile.Name())
	assert.NoError(t, err)

	// Remove PID file
	removePIDFile(tmpFile.Name())

	// Verify file is removed
	_, err = os.Stat(tmpFile.Name())
	assert.True(t, os.IsNotExist(err))
}

func TestRemovePIDFileEmpty(t *testing.T) {
	// Should not panic or error
	removePIDFile("")
}

func TestDefaultConfigurationLoad(t *testing.T) {
	// Test loading default configuration (no config file)
	config, err := loadConfiguration("")
	require.NoError(t, err)

	// Should return default configuration
	defaultConfig := k2m.DefaultConfig()
	assert.Equal(t, defaultConfig.KafkaConfig.Brokers, config.KafkaConfig.Brokers)
	assert.Equal(t, defaultConfig.KafkaConfig.Topics, config.KafkaConfig.Topics)
	assert.Equal(t, defaultConfig.MQTTConfig.Broker, config.MQTTConfig.Broker)
	assert.Equal(t, defaultConfig.WorkerCount, config.WorkerCount)
	assert.Equal(t, defaultConfig.BufferSize, config.BufferSize)
}

// Benchmark tests for performance
func BenchmarkParseTopicMappings(b *testing.B) {
	mappingsStr := "topic1=mqtt1,topic2=mqtt2,topic3=mqtt3,topic4=mqtt4,topic5=mqtt5"
	transform := "json"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseTopicMappings(mappingsStr, transform)
	}
}

func BenchmarkLoadConfigurationFromFile(b *testing.B) {
	// Create a config file for benchmarking
	configData := k2m.DefaultConfig()
	configJSON, _ := json.Marshal(configData)

	tmpFile, _ := os.CreateTemp("", "k2m-bench-config-*.json")
	tmpFile.Write(configJSON)
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loadConfiguration(tmpFile.Name())
	}
}
