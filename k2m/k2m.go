package k2m

import (
	"actsvr/util"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// K2MConfig holds configuration for the Kafka to MQTT broker
type K2MConfig struct {
	// Kafka consumer configuration
	KafkaConfig KafkaConfig `json:"kafka"`
	// MQTT publisher configuration
	MQTTConfig MQTTConfig `json:"mqtt"`
	// Topic mapping configuration (deprecated, use Routes instead)
	TopicMappings []TopicMapping `json:"topicMappings,omitempty"`
	// Routing configuration
	Routes []RouteConfig `json:"routes,omitempty"`
	// Worker configuration
	WorkerCount int `json:"workerCount"`
	BufferSize  int `json:"bufferSize"`
	// Health check configuration
	HttpConfig HttpConfig `json:"http"`
}

// KafkaConfig holds Kafka consumer settings
type KafkaConfig struct {
	Brokers       []string `json:"brokers"`
	Topics        []string `json:"topics"`
	ConsumerGroup string   `json:"consumerGroup"`
	// Consumer configuration
	ReturnErrors      bool     `json:"returnErrors"`
	OffsetOldest      bool     `json:"offsetOldest"`
	SessionTimeout    Duration `json:"sessionTimeout"`
	HeartbeatInterval Duration `json:"heartbeatInterval"`
}

// MQTTConfig holds MQTT publisher settings
type MQTTConfig struct {
	Broker   string `json:"broker"`
	ClientID string `json:"clientId"`
	Username string `json:"username"`
	Password string `json:"password"`
	QoS      byte   `json:"qos"`
	Retained bool   `json:"retained"`
	// Connection configuration
	KeepAlive            Duration `json:"keepAlive"`
	PingTimeout          Duration `json:"pingTimeout"`
	ConnectTimeout       Duration `json:"connectTimeout"`
	ConnectRetry         bool     `json:"connectRetry"`
	MaxReconnectInterval Duration `json:"maxReconnectInterval"`
}

// TopicMapping defines how to map Kafka topics to MQTT topics
type TopicMapping struct {
	KafkaTopic string `json:"kafkaTopic"`
	MQTTTopic  string `json:"mqttTopic"`
	Transform  string `json:"transform"` // "none", "json", "custom"
}

type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		var num int64
		if err := json.Unmarshal(b, &num); err != nil {
			return err
		}
		*d = Duration(num)
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// K2MBroker is a bridge from Kafka to MQTT.
// It consumes from Kafka and publishes to MQTT.
type K2MBroker struct {
	config *K2MConfig
	logger *util.Log

	// Kafka components
	consumerGroup sarama.ConsumerGroup
	consumer      *Consumer

	// MQTT components
	mqttClient mqtt.Client

	// Control channels
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan bool
	wg     sync.WaitGroup

	// Message processing
	messageCh chan *sarama.ConsumerMessage
	workers   []*MessageWorker

	// Routing system
	router *MessageRouter

	// Metrics and monitoring
	metrics       *Metrics
	healthChecker *HealthChecker
}

// Consumer represents the Sarama consumer group consumer
type Consumer struct {
	ready  chan bool
	broker *K2MBroker
}

// MessageWorker processes messages from Kafka and publishes to MQTT
type MessageWorker struct {
	id        int
	broker    *K2MBroker
	messageCh <-chan *sarama.ConsumerMessage
}

// DefaultConfig returns a default configuration for the K2M broker
func DefaultConfig() *K2MConfig {
	return &K2MConfig{
		KafkaConfig: KafkaConfig{
			Brokers:           []string{"localhost:9092"},
			Topics:            []string{"test-topic"},
			ConsumerGroup:     "k2m-consumer-group",
			ReturnErrors:      true,
			OffsetOldest:      true,
			SessionTimeout:    Duration(10 * time.Second),
			HeartbeatInterval: Duration(3 * time.Second),
		},
		MQTTConfig: MQTTConfig{
			Broker:               "tcp://localhost:1883",
			ClientID:             "k2m-broker",
			QoS:                  1,
			Retained:             false,
			KeepAlive:            Duration(60 * time.Second),
			PingTimeout:          Duration(1 * time.Second),
			ConnectRetry:         true,
			ConnectTimeout:       Duration(3 * time.Second),
			MaxReconnectInterval: Duration(10 * time.Minute),
		},
		TopicMappings: []TopicMapping{
			{
				KafkaTopic: "sensor-data",
				MQTTTopic:  "iot/sensors/{key}",
				Transform:  "json",
			},
			{
				KafkaTopic: "user-events",
				MQTTTopic:  "events/users/{kafkaTopic}",
				Transform:  "none",
			},
		},
		Routes:      DefaultRouteConfig(),
		WorkerCount: 5,
		BufferSize:  1000,
		HttpConfig: HttpConfig{
			Enabled: false,
			Host:    "0.0.0.0",
			Port:    8080,
		},
	}
}

// NewK2MBroker creates a new Kafka to MQTT broker instance
func NewK2MBroker(config *K2MConfig, logger *util.Log) (*K2MBroker, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	broker := &K2MBroker{
		config:    config,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		ready:     make(chan bool),
		messageCh: make(chan *sarama.ConsumerMessage, config.BufferSize),
		metrics:   NewMetrics(),
	}

	// Initialize routing system
	routes := config.Routes
	if len(config.TopicMappings) > 0 {
		// Convert legacy TopicMappings to Routes for backward compatibility
		for i, mapping := range config.TopicMappings {
			routes = append(routes, RouteConfig{
				Name:     fmt.Sprintf("route_topic_%d", i),
				Priority: 1,
				Filters: []FilterConfig{
					{
						Type: "topic",
						Config: map[string]interface{}{
							"pattern": mapping.KafkaTopic,
						},
					},
				},
				Mapping: mapping,
			})
		}
	}
	if len(routes) == 0 {
		routes = DefaultRouteConfig()
	}

	if err := ValidateRouteConfig(routes); err != nil {
		return nil, fmt.Errorf("invalid route configuration: %w", err)
	}

	router, err := NewMessageRouter(routes)
	if err != nil {
		return nil, fmt.Errorf("failed to create message router: %w", err)
	}
	broker.router = router

	// Initialize health checker
	broker.healthChecker = NewHealthChecker(broker, config.HttpConfig)

	return broker, nil
}

// Start starts the K2M broker
func (b *K2MBroker) Start() error {
	b.logger.Infof("Starting K2M Broker")

	// Initialize MQTT client
	if err := b.initMQTTClient(); err != nil {
		return fmt.Errorf("failed to initialize MQTT client: %w", err)
	}
	b.logger.Infof("init mqtt client")

	// Initialize Kafka consumer
	if err := b.initKafkaConsumer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka consumer: %w", err)
	}
	b.logger.Infof("init kafka client")

	// Start message workers
	b.startMessageWorkers()
	b.logger.Infof("start workers")

	// Start consuming from Kafka
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			if err := b.consumerGroup.Consume(b.ctx, b.config.KafkaConfig.Topics, b.consumer); err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				b.logger.Errorf("Error from consumer: %v", err)
			}
			if b.ctx.Err() != nil {
				return
			}
			b.consumer.ready = make(chan bool)
		}
	}()

	<-b.consumer.ready

	// Start metrics update routine
	b.wg.Add(1)
	go b.metricsUpdateLoop()

	// Start health check server
	if err := b.healthChecker.Start(); err != nil {
		return fmt.Errorf("failed to start health check server: %w", err)
	}

	b.logger.Infof("K2M Broker started and ready")
	return nil
}

// Stop stops the K2M broker
func (b *K2MBroker) Stop() error {
	b.logger.Infof("Stopping K2M Broker")

	// Cancel context to stop all goroutines
	b.cancel()

	// Stop health check server
	if b.healthChecker != nil {
		if err := b.healthChecker.Stop(); err != nil {
			b.logger.Errorf("Error stopping health check server: %v", err)
		}
	}

	// Close Kafka consumer
	if b.consumerGroup != nil {
		if err := b.consumerGroup.Close(); err != nil {
			b.logger.Errorf("Error closing consumer group: %v", err)
		}
		b.metrics.SetKafkaConnected(false)
	}

	// Close MQTT client
	if b.mqttClient != nil && b.mqttClient.IsConnected() {
		b.mqttClient.Disconnect(250)
		b.metrics.SetMQTTConnected(false)
	}

	// Close message channel
	close(b.messageCh)

	// Wait for all goroutines to finish
	b.wg.Wait()

	b.logger.Infof("K2M Broker stopped")
	return nil
}

// initMQTTClient initializes the MQTT client
func (b *K2MBroker) initMQTTClient() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(b.config.MQTTConfig.Broker)
	opts.SetClientID(b.config.MQTTConfig.ClientID)

	if b.config.MQTTConfig.Username != "" {
		opts.SetUsername(b.config.MQTTConfig.Username)
	}
	if b.config.MQTTConfig.Password != "" {
		opts.SetPassword(b.config.MQTTConfig.Password)
	}

	opts.SetKeepAlive(time.Duration(b.config.MQTTConfig.KeepAlive))
	opts.SetPingTimeout(time.Duration(b.config.MQTTConfig.PingTimeout))
	opts.SetConnectRetry(b.config.MQTTConfig.ConnectRetry)
	opts.SetMaxReconnectInterval(time.Duration(b.config.MQTTConfig.MaxReconnectInterval))

	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		b.logger.Debugf("Received unexpected message: %s from topic: %s", msg.Payload(), msg.Topic())
	})

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		b.logger.Infof("Connected to MQTT broker: %s", b.config.MQTTConfig.Broker)
		b.metrics.SetMQTTConnected(true)
	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		b.logger.Errorf("Connection to MQTT broker lost: %v", err)
		b.metrics.SetMQTTConnected(false)
		b.metrics.IncrementMQTTErrors()
	})

	b.mqttClient = mqtt.NewClient(opts)

	token := b.mqttClient.Connect()
	if token.WaitTimeout(time.Duration(b.config.MQTTConfig.ConnectTimeout)) && token.Error() != nil {
		b.metrics.SetMQTTConnected(false)
		return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
	}

	b.metrics.SetMQTTConnected(true)
	b.logger.Infof("Connected to MQTT broker: %s", b.config.MQTTConfig.Broker)
	return nil
}

// initKafkaConsumer initializes the Kafka consumer
func (b *K2MBroker) initKafkaConsumer() error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Session.Timeout = time.Duration(b.config.KafkaConfig.SessionTimeout)
	config.Consumer.Group.Heartbeat.Interval = time.Duration(b.config.KafkaConfig.HeartbeatInterval)
	config.Consumer.Return.Errors = b.config.KafkaConfig.ReturnErrors

	if b.config.KafkaConfig.OffsetOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	consumerGroup, err := sarama.NewConsumerGroup(b.config.KafkaConfig.Brokers, b.config.KafkaConfig.ConsumerGroup, config)
	if err != nil {
		return fmt.Errorf("error creating consumer group client: %w", err)
	}

	b.consumerGroup = consumerGroup
	b.consumer = &Consumer{
		ready:  make(chan bool),
		broker: b,
	}

	// Track errors
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for err := range b.consumerGroup.Errors() {
			b.logger.Errorf("Consumer error: %v", err)
			b.metrics.IncrementKafkaErrors()
		}
	}()

	b.metrics.SetKafkaConnected(true)
	b.logger.Infof("Kafka consumer initialized for topics: %v", b.config.KafkaConfig.Topics)
	return nil
}

// startMessageWorkers starts the message processing workers
func (b *K2MBroker) startMessageWorkers() {
	b.workers = make([]*MessageWorker, b.config.WorkerCount)

	for i := 0; i < b.config.WorkerCount; i++ {
		worker := &MessageWorker{
			id:        i,
			broker:    b,
			messageCh: b.messageCh,
		}
		b.workers[i] = worker

		b.wg.Add(1)
		go worker.start()
	}

	b.metrics.SetActiveWorkers(b.config.WorkerCount)
	b.logger.Infof("Started %d message workers", b.config.WorkerCount)
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				consumer.broker.logger.Infof("Message channel closed")
				return nil
			}
			if message == nil {
				continue
			}

			// Track message received
			consumer.broker.metrics.IncrementMessagesReceived()

			// Send message to workers for processing
			select {
			case consumer.broker.messageCh <- message:
				// Message sent to worker
			case <-consumer.broker.ctx.Done():
				return nil
			default:
				consumer.broker.logger.Warnf("Message buffer full, dropping message from topic %s", message.Topic)
				consumer.broker.metrics.IncrementMessagesDropped()
			}

			// Mark message as processed
			session.MarkMessage(message, "")

		case <-consumer.broker.ctx.Done():
			return nil
		}
	}
}

// start starts the message worker
func (w *MessageWorker) start() {
	defer w.broker.wg.Done()
	w.broker.logger.Infof("Message worker %d started", w.id)

	for {
		select {
		case message, ok := <-w.messageCh:
			if !ok {
				w.broker.logger.Infof("Message worker %d stopped", w.id)
				return
			}
			w.processMessage(message)

		case <-w.broker.ctx.Done():
			w.broker.logger.Infof("Message worker %d stopped", w.id)
			return
		}
	}
}

// processMessage processes a Kafka message and publishes it to MQTT
func (w *MessageWorker) processMessage(message *sarama.ConsumerMessage) {
	startTime := time.Now()

	// Find matching route using the router
	route := w.broker.router.FindRoute(message)
	if route == nil {
		w.broker.logger.Warnf("No route found for Kafka topic: %s", message.Topic)
		w.broker.metrics.IncrementMessagesFailed()
		return
	}

	mapping := &route.Mapping

	// Transform message payload
	payload, err := w.transformMessage(message, mapping)
	if err != nil {
		w.broker.logger.Errorf("Failed to transform message: %v", err)
		w.broker.metrics.IncrementTransformErrors()
		w.broker.metrics.IncrementMessagesFailed()
		return
	}

	// Record processing latency
	processTime := time.Since(startTime)
	w.broker.metrics.RecordProcessingLatency(processTime)
	w.broker.metrics.IncrementMessagesProcessed()

	// Publish to MQTT
	publishStart := time.Now()
	mqttTopic := w.resolveMQTTTopic(mapping.MQTTTopic, message)
	token := w.broker.mqttClient.Publish(
		mqttTopic,
		w.broker.config.MQTTConfig.QoS,
		w.broker.config.MQTTConfig.Retained,
		payload,
	)

	// Wait for publish to complete or timeout
	if !token.WaitTimeout(5 * time.Second) {
		w.broker.logger.Errorf("MQTT publish timeout for topic: %s", mqttTopic)
		w.broker.metrics.IncrementPublishTimeouts()
		w.broker.metrics.IncrementMessagesFailed()
		return
	}

	if token.Error() != nil {
		w.broker.logger.Errorf("MQTT publish failed: %v", token.Error())
		w.broker.metrics.IncrementMQTTErrors()
		w.broker.metrics.IncrementMessagesFailed()
		return
	}

	// Record publish latency and success
	publishTime := time.Since(publishStart)
	w.broker.metrics.RecordPublishLatency(publishTime)
	w.broker.metrics.IncrementMessagesPublished()

	w.broker.logger.Debugf("Published message to MQTT topic: %s", mqttTopic)
}

// transformMessage transforms the Kafka message according to the mapping configuration
func (w *MessageWorker) transformMessage(message *sarama.ConsumerMessage, mapping *TopicMapping) ([]byte, error) {
	switch mapping.Transform {
	case "none":
		return message.Value, nil

	case "json":
		// Wrap the message in a JSON envelope with metadata
		envelope := map[string]interface{}{
			"kafkaTopic":     message.Topic,
			"kafkaPartition": message.Partition,
			"kafkaOffset":    message.Offset,
			"timestamp":      message.Timestamp,
			"key":            string(message.Key),
			"value":          string(message.Value),
		}
		return json.Marshal(envelope)

	default:
		// Default to no transformation
		return message.Value, nil
	}
}

// resolveMQTTTopic resolves the MQTT topic, supporting simple templating
func (w *MessageWorker) resolveMQTTTopic(template string, message *sarama.ConsumerMessage) string {
	topic := template
	topic = strings.ReplaceAll(topic, "{kafkaTopic}", message.Topic)
	topic = strings.ReplaceAll(topic, "{partition}", fmt.Sprintf("%d", message.Partition))
	topic = strings.ReplaceAll(topic, "{key}", string(message.Key))
	return topic
}

// GetMetrics returns a snapshot of the current metrics
func (b *K2MBroker) GetMetrics() Metrics {
	return b.metrics.GetSnapshot()
}

// IsHealthy returns true if the broker is healthy
func (b *K2MBroker) IsHealthy() bool {
	return b.metrics.IsHealthy()
}

// metricsUpdateLoop periodically updates metrics rates and buffer utilization
func (b *K2MBroker) metricsUpdateLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Update throughput rates
			b.metrics.UpdateRates()

			// Update buffer utilization
			bufferLen := len(b.messageCh)
			bufferCap := cap(b.messageCh)
			utilization := float64(bufferLen) / float64(bufferCap)
			b.metrics.SetBufferUtilization(utilization)

		case <-b.ctx.Done():
			return
		}
	}
}
