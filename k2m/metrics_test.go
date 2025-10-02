package k2m

import (
	"actsvr/util"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	metrics := NewMetrics()
	assert.NotNil(t, metrics)
	assert.False(t, metrics.StartTime.IsZero())
	assert.Equal(t, int64(0), metrics.MessagesReceived)
	assert.Equal(t, int64(0), metrics.MessagesProcessed)
}

func TestMetricsCounters(t *testing.T) {
	metrics := NewMetrics()

	// Test increment methods
	metrics.IncrementMessagesReceived()
	metrics.IncrementMessagesProcessed()
	metrics.IncrementMessagesPublished()
	metrics.IncrementMessagesFailed()
	metrics.IncrementMessagesDropped()
	metrics.IncrementKafkaErrors()
	metrics.IncrementMQTTErrors()
	metrics.IncrementTransformErrors()

	snapshot := metrics.GetSnapshot()
	assert.Equal(t, int64(1), snapshot.MessagesReceived)
	assert.Equal(t, int64(1), snapshot.MessagesProcessed)
	assert.Equal(t, int64(1), snapshot.MessagesPublished)
	assert.Equal(t, int64(1), snapshot.MessagesFailed)
	assert.Equal(t, int64(1), snapshot.MessagesDropped)
	assert.Equal(t, int64(1), snapshot.KafkaErrors)
	assert.Equal(t, int64(1), snapshot.MQTTErrors)
	assert.Equal(t, int64(1), snapshot.TransformErrors)
}

func TestMetricsLatency(t *testing.T) {
	metrics := NewMetrics()

	processingDuration := 100 * time.Microsecond
	publishDuration := 200 * time.Microsecond

	metrics.RecordProcessingLatency(processingDuration)
	metrics.RecordPublishLatency(publishDuration)

	snapshot := metrics.GetSnapshot()
	assert.Equal(t, int64(100), snapshot.ProcessingLatency)
	assert.Equal(t, int64(200), snapshot.PublishLatency)
}

func TestMetricsConnectionStatus(t *testing.T) {
	metrics := NewMetrics()

	// Test initial state
	snapshot := metrics.GetSnapshot()
	assert.False(t, snapshot.KafkaConnected)
	assert.False(t, snapshot.MQTTConnected)

	// Test setting connected
	metrics.SetKafkaConnected(true)
	metrics.SetMQTTConnected(true)

	snapshot = metrics.GetSnapshot()
	assert.True(t, snapshot.KafkaConnected)
	assert.True(t, snapshot.MQTTConnected)

	// Test health check
	assert.True(t, metrics.IsHealthy())

	// Test disconnected
	metrics.SetKafkaConnected(false)
	assert.False(t, metrics.IsHealthy())
}

func TestMetricsWorkerStatus(t *testing.T) {
	metrics := NewMetrics()

	metrics.SetActiveWorkers(5)
	metrics.SetBufferUtilization(0.75)

	snapshot := metrics.GetSnapshot()
	assert.Equal(t, 5, snapshot.ActiveWorkers)
	assert.Equal(t, 0.75, snapshot.BufferUtilization)
}

func TestMetricsUpdateRates(t *testing.T) {
	metrics := NewMetrics()

	// Simulate some activity
	for i := 0; i < 10; i++ {
		metrics.IncrementMessagesReceived()
		metrics.IncrementMessagesProcessed()
		metrics.IncrementMessagesPublished()
	}

	// Wait a bit for time to pass
	time.Sleep(10 * time.Millisecond)

	// Update rates
	metrics.UpdateRates()

	snapshot := metrics.GetSnapshot()
	// Rates should be calculated (exact values depend on timing)
	assert.GreaterOrEqual(t, snapshot.ReceiveRate, 0.0)
	assert.GreaterOrEqual(t, snapshot.ProcessRate, 0.0)
	assert.GreaterOrEqual(t, snapshot.PublishRate, 0.0)
}

func TestMetricsErrorRate(t *testing.T) {
	metrics := NewMetrics()

	// No messages, should be 0% error rate
	assert.Equal(t, 0.0, metrics.GetErrorRate())

	// Add some messages and errors
	for i := 0; i < 100; i++ {
		metrics.IncrementMessagesReceived()
	}
	for i := 0; i < 10; i++ {
		metrics.IncrementMessagesFailed()
	}

	errorRate := metrics.GetErrorRate()
	assert.Equal(t, 10.0, errorRate) // 10 errors out of 100 messages = 10%
}

func TestMetricsReset(t *testing.T) {
	metrics := NewMetrics()

	// Add some data
	metrics.IncrementMessagesReceived()
	metrics.IncrementMessagesFailed()
	metrics.SetKafkaConnected(true)

	// Reset
	metrics.Reset()

	snapshot := metrics.GetSnapshot()
	assert.Equal(t, int64(0), snapshot.MessagesReceived)
	assert.Equal(t, int64(0), snapshot.MessagesFailed)
	assert.Equal(t, 0.0, snapshot.ReceiveRate)
}

func TestMetricsUptime(t *testing.T) {
	metrics := NewMetrics()

	// Wait a small amount of time
	time.Sleep(10 * time.Millisecond)

	uptime := metrics.Uptime()
	assert.Greater(t, uptime, time.Duration(0))
	assert.Less(t, uptime, 1*time.Second) // Should be very small
}

func TestHealthChecker(t *testing.T) {
	// Create a test broker with mock components
	config := DefaultConfig()
	config.HttpConfig.Enabled = true
	config.HttpConfig.Host = "localhost"
	config.HttpConfig.Port = 0 // Use random port for testing

	logger := &util.Log{}
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	// Set some metrics for testing
	broker.metrics.SetKafkaConnected(true)
	broker.metrics.SetMQTTConnected(true)
	broker.metrics.SetActiveWorkers(5)
	broker.metrics.IncrementMessagesReceived()

	// Create health checker
	healthChecker := NewHealthChecker(broker, config.HttpConfig)

	// Test health check endpoint
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	healthChecker.handleHealthCheck(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var response HealthCheckResponse
	err = json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "healthy", response.Status)
	assert.NotEmpty(t, response.Uptime)
	assert.True(t, response.Metrics.KafkaConnected)
	assert.True(t, response.Metrics.MQTTConnected)
	assert.NotEmpty(t, response.Checks)
}

func TestHealthCheckerMetricsEndpoint(t *testing.T) {
	config := DefaultConfig()
	logger := &util.Log{}
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	healthChecker := NewHealthChecker(broker, config.HttpConfig)

	req := httptest.NewRequest(http.MethodGet, "/health/metrics", nil)
	w := httptest.NewRecorder()

	healthChecker.handleMetrics(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

	var metrics Metrics
	err = json.Unmarshal(w.Body.Bytes(), &metrics)
	require.NoError(t, err)

	assert.False(t, metrics.StartTime.IsZero())
}

func TestHealthCheckerStatusEndpoint(t *testing.T) {
	config := DefaultConfig()
	logger := &util.Log{}
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	// Set unhealthy state
	broker.metrics.SetKafkaConnected(false)
	broker.metrics.SetMQTTConnected(false)

	healthChecker := NewHealthChecker(broker, config.HttpConfig)

	req := httptest.NewRequest(http.MethodGet, "/health/status", nil)
	w := httptest.NewRecorder()

	healthChecker.handleStatus(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	var status map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &status)
	require.NoError(t, err)

	assert.Equal(t, "unhealthy", status["status"])
	assert.Contains(t, status, "checks")
}

func TestHealthChecks(t *testing.T) {
	config := DefaultConfig()
	logger := &util.Log{}
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	healthChecker := NewHealthChecker(broker, config.HttpConfig)

	// Test with healthy state
	broker.metrics.SetKafkaConnected(true)
	broker.metrics.SetMQTTConnected(true)
	broker.metrics.SetActiveWorkers(5)
	broker.metrics.SetBufferUtilization(0.5)

	checks := healthChecker.performHealthChecks()
	assert.NotEmpty(t, checks)

	// Verify all checks are present
	checkNames := make(map[string]bool)
	for _, check := range checks {
		checkNames[check.Name] = true
		if check.Name == "kafka" || check.Name == "mqtt" || check.Name == "workers" {
			assert.Equal(t, "healthy", check.Status)
		}
	}

	expectedChecks := []string{"kafka", "mqtt", "workers", "processing", "buffer", "activity"}
	for _, expected := range expectedChecks {
		assert.True(t, checkNames[expected], "Missing check: %s", expected)
	}
}

func TestHealthChecksUnhealthyStates(t *testing.T) {
	config := DefaultConfig()
	logger := &util.Log{}
	broker, err := NewK2MBroker(config, logger)
	require.NoError(t, err)

	healthChecker := NewHealthChecker(broker, config.HttpConfig)

	// Test with unhealthy states
	broker.metrics.SetKafkaConnected(false)
	broker.metrics.SetMQTTConnected(false)
	broker.metrics.SetActiveWorkers(0)
	broker.metrics.SetBufferUtilization(0.99) // Critical utilization

	checks := healthChecker.performHealthChecks()

	// Find specific checks and verify their status
	for _, check := range checks {
		switch check.Name {
		case "kafka":
			assert.Equal(t, "unhealthy", check.Status)
		case "mqtt":
			assert.Equal(t, "unhealthy", check.Status)
		case "workers":
			assert.Equal(t, "unhealthy", check.Status)
		case "buffer":
			assert.Equal(t, "unhealthy", check.Status)
		}
	}
}

func BenchmarkMetricsIncrement(b *testing.B) {
	metrics := NewMetrics()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.IncrementMessagesReceived()
	}
}

func BenchmarkMetricsSnapshot(b *testing.B) {
	metrics := NewMetrics()

	// Add some data
	for i := 0; i < 1000; i++ {
		metrics.IncrementMessagesReceived()
		metrics.IncrementMessagesProcessed()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.GetSnapshot()
	}
}

func BenchmarkHealthCheck(b *testing.B) {
	config := DefaultConfig()
	logger := &util.Log{}
	broker, _ := NewK2MBroker(config, logger)

	broker.metrics.SetKafkaConnected(true)
	broker.metrics.SetMQTTConnected(true)
	broker.metrics.SetActiveWorkers(5)

	healthChecker := NewHealthChecker(broker, config.HttpConfig)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = healthChecker.performHealthChecks()
	}
}
