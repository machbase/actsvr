package main

import (
	"actsvr/k2m"
	"actsvr/util"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var (
	// General flags
	configFile  = flag.String("config", "", "Path to configuration file (JSON)")
	logFilename = flag.String("log-file", "-", "Log file path (default: stdout)")
	logLevel    = flag.Int("log-level", 1, "Log verbosity level (0=quiet, 1=info, 2=debug)")
	pidFile     = flag.String("pid", "", "PID file path")

	// Kafka flags
	kafkaBrokers  = flag.String("kafka-brokers", "localhost:9092", "Comma-separated list of Kafka brokers")
	kafkaTopics   = flag.String("kafka-topics", "test-topic", "Comma-separated list of Kafka topics to consume")
	consumerGroup = flag.String("consumer-group", "k2m-consumer-group", "Kafka consumer group ID")
	offsetOldest  = flag.Bool("offset-oldest", true, "Start consuming from the oldest offset")

	// MQTT flags
	mqttBroker   = flag.String("mqtt-broker", "tcp://localhost:1883", "MQTT broker URL")
	mqttClientID = flag.String("mqtt-client-id", "k2m-broker", "MQTT client ID")
	mqttUsername = flag.String("mqtt-username", "", "MQTT username")
	mqttPassword = flag.String("mqtt-password", "", "MQTT password")
	mqttQoS      = flag.Int("mqtt-qos", 1, "MQTT QoS level (0, 1, or 2)")
	mqttRetained = flag.Bool("mqtt-retained", false, "Publish MQTT messages as retained")

	// Topic mapping flags
	topicMappings    = flag.String("topic-mappings", "test-topic=mqtt/test", "Comma-separated topic mappings (kafka-topic=mqtt-topic)")
	messageTransform = flag.String("message-transform", "none", "Message transformation (none, json)")

	// Worker flags
	workerCount = flag.Int("workers", 5, "Number of message processing workers")
	bufferSize  = flag.Int("buffer-size", 1000, "Message buffer size")

	// Health check flags
	httpEnabled = flag.Bool("http-enabled", false, "Enable HTTP server")
	httpHost    = flag.String("http-host", "", "HTTP server host")
	httpPort    = flag.Int("http-port", 8080, "HTTP server port")
)

func main() {
	flag.Parse()

	// Create logger
	logger := createLogger()

	// Write PID file
	if *pidFile != "" {
		if err := writePIDFile(*pidFile); err != nil {
			logger.Errorf("Failed to write PID file: %v", err)
			os.Exit(1)
		}
		defer removePIDFile(*pidFile)
	}

	// Load configuration
	config, err := loadConfiguration(*configFile)
	if err != nil {
		logger.Errorf("Failed to load configuration: %v", err)
		os.Exit(1)
	}
	// Override with command line flags
	applyCommandLineFlags(config)

	// Create and start the broker
	broker, err := k2m.NewK2MBroker(config, logger)
	if err != nil {
		logger.Errorf("Failed to create broker: %v", err)
		os.Exit(1)
	}

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the broker
	if err := broker.Start(); err != nil {
		logger.Errorf("Failed to start broker: %v", err)
		os.Exit(1)
	}

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Infof("Received signal %v, shutting down...", sig)

	// Stop the broker
	if err := broker.Stop(); err != nil {
		logger.Errorf("Error stopping broker: %v", err)
		os.Exit(1)
	}

	logger.Infof("K2M Broker stopped gracefully")
}

func createLogger() *util.Log {
	cfg := util.DefaultLogConfig()
	cfg.Filename = *logFilename
	logger := util.NewLog(cfg)
	logger.SetVerbose(*logLevel)
	return logger
}

func loadConfiguration(configFile string) (*k2m.K2MConfig, error) {
	var config *k2m.K2MConfig

	// Load from file if specified
	if configFile != "" {
		configData, err := os.ReadFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		config = &k2m.K2MConfig{}
		if err := json.Unmarshal(configData, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	} else {
		// Use default config and override with command line flags
		config = k2m.DefaultConfig()
	}

	return config, nil
}

func applyCommandLineFlags(config *k2m.K2MConfig) {
	// Kafka configuration
	if flag.Lookup("kafka-brokers").Value.String() != flag.Lookup("kafka-brokers").DefValue {
		config.KafkaConfig.Brokers = strings.Split(*kafkaBrokers, ",")
	}
	if flag.Lookup("kafka-topics").Value.String() != flag.Lookup("kafka-topics").DefValue {
		config.KafkaConfig.Topics = strings.Split(*kafkaTopics, ",")
	}
	if flag.Lookup("consumer-group").Value.String() != flag.Lookup("consumer-group").DefValue {
		config.KafkaConfig.ConsumerGroup = *consumerGroup
	}
	if flag.Lookup("offset-oldest").Value.String() != flag.Lookup("offset-oldest").DefValue {
		config.KafkaConfig.OffsetOldest = *offsetOldest
	}

	// MQTT configuration
	if flag.Lookup("mqtt-broker").Value.String() != flag.Lookup("mqtt-broker").DefValue {
		config.MQTTConfig.Broker = *mqttBroker
	}
	if flag.Lookup("mqtt-client-id").Value.String() != flag.Lookup("mqtt-client-id").DefValue {
		config.MQTTConfig.ClientID = *mqttClientID
	}
	if *mqttUsername != "" {
		config.MQTTConfig.Username = *mqttUsername
	}
	if *mqttPassword != "" {
		config.MQTTConfig.Password = *mqttPassword
	}
	if flag.Lookup("mqtt-qos").Value.String() != flag.Lookup("mqtt-qos").DefValue {
		config.MQTTConfig.QoS = byte(*mqttQoS)
	}
	if flag.Lookup("mqtt-retained").Value.String() != flag.Lookup("mqtt-retained").DefValue {
		config.MQTTConfig.Retained = *mqttRetained
	}

	// Topic mappings
	if flag.Lookup("topic-mappings").Value.String() != flag.Lookup("topic-mappings").DefValue {
		config.TopicMappings = parseTopicMappings(*topicMappings, *messageTransform)
	}

	// Worker configuration
	if flag.Lookup("workers").Value.String() != flag.Lookup("workers").DefValue {
		config.WorkerCount = *workerCount
	}
	if flag.Lookup("buffer-size").Value.String() != flag.Lookup("buffer-size").DefValue {
		config.BufferSize = *bufferSize
	}

	// Health check configuration
	if flag.Lookup("http-enabled").Value.String() != flag.Lookup("http-enabled").DefValue {
		config.HttpConfig.Enabled = *httpEnabled
	}
	if flag.Lookup("http-host").Value.String() != flag.Lookup("http-host").DefValue && *httpHost != "" {
		config.HttpConfig.Host = *httpHost
	}
	if flag.Lookup("http-port").Value.String() != flag.Lookup("http-port").DefValue {
		config.HttpConfig.Port = *httpPort
	}
}

func parseTopicMappings(mappingsStr, transform string) []k2m.TopicMapping {
	var mappings = []k2m.TopicMapping{}

	if mappingsStr == "" {
		return mappings
	}

	parts := strings.Split(mappingsStr, ",")
	for _, part := range parts {
		mapping := strings.Split(strings.TrimSpace(part), "=")
		if len(mapping) == 2 {
			mappings = append(mappings, k2m.TopicMapping{
				KafkaTopic: strings.TrimSpace(mapping[0]),
				MQTTTopic:  strings.TrimSpace(mapping[1]),
				Transform:  transform,
			})
		}
	}

	return mappings
}

func writePIDFile(pidFile string) error {
	if pidFile == "" {
		return nil
	}

	file, err := os.Create(pidFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d\n", os.Getpid())
	return err
}

func removePIDFile(pidFile string) {
	if pidFile != "" {
		os.Remove(pidFile)
	}
}
