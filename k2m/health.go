package k2m

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HttpConfig holds configuration for health check HTTP server
type HttpConfig struct {
	Enabled bool   `json:"enabled"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
}

// HealthChecker provides HTTP endpoints for health checks and metrics
type HealthChecker struct {
	broker *K2MBroker
	config HttpConfig
	server *http.Server
}

// HealthStatus represents the overall health status
type HealthStatus struct {
	Status  string                    `json:"status"`
	Checks  map[string]ComponentCheck `json:"checks"`
	Uptime  string                    `json:"uptime"`
	Version string                    `json:"version"`
}

// ComponentCheck represents the health of a specific component
type ComponentCheck struct {
	Status      string      `json:"status"`
	Connected   bool        `json:"connected,omitempty"`
	LastMessage *time.Time  `json:"lastMessage,omitempty"`
	Active      int         `json:"active,omitempty"`
	Total       int         `json:"total,omitempty"`
	Utilization float64     `json:"utilization,omitempty"`
	Size        int         `json:"size,omitempty"`
	Details     interface{} `json:"details,omitempty"`
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(broker *K2MBroker, config HttpConfig) *HealthChecker {
	return &HealthChecker{
		broker: broker,
		config: config,
	}
}

// Start starts the health check HTTP server
func (hc *HealthChecker) Start() error {
	if !hc.config.Enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", hc.healthHandler)
	mux.HandleFunc("/metrics", hc.metricsHandler)
	mux.HandleFunc("/status", hc.statusHandler)

	addr := fmt.Sprintf("%s:%d", hc.config.Host, hc.config.Port)
	hc.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := hc.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hc.broker.logger.Errorf("Health check server error: %v", err)
		}
	}()

	hc.broker.logger.Infof("Health check server started on %s", addr)
	return nil
}

// Stop stops the health check HTTP server
func (hc *HealthChecker) Stop() error {
	if hc.server == nil {
		return nil
	}
	return hc.server.Close()
}

// healthHandler handles the health check endpoint
func (hc *HealthChecker) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	health := hc.performAllHealthChecks()

	// Set HTTP status based on overall health
	statusCode := http.StatusOK
	if health.Status != "healthy" {
		statusCode = http.StatusServiceUnavailable
	}

	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(health)
}

// metricsHandler handles the metrics endpoint
func (hc *HealthChecker) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	metrics := hc.broker.metrics.GetSnapshot()
	json.NewEncoder(w).Encode(&metrics)
}

// statusHandler handles the status endpoint
func (hc *HealthChecker) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	status := map[string]interface{}{
		"broker":    "k2m-broker",
		"version":   "2.0.0",
		"uptime":    hc.broker.metrics.Uptime().String(),
		"startTime": hc.broker.metrics.StartTime,
		"config": map[string]interface{}{
			"workerCount": hc.broker.config.WorkerCount,
			"bufferSize":  hc.broker.config.BufferSize,
			"routeCount":  len(hc.broker.config.Routes),
		},
	}

	json.NewEncoder(w).Encode(status)
}

// performAllHealthChecks performs all health checks and returns the overall status
func (hc *HealthChecker) performAllHealthChecks() *HealthStatus {
	checks := make(map[string]ComponentCheck)

	// Check Kafka connection
	kafkaCheck := hc.checkKafkaHealth()
	checks["kafka"] = kafkaCheck

	// Check MQTT connection
	mqttCheck := hc.checkMQTTHealth()
	checks["mqtt"] = mqttCheck

	// Check workers
	workersCheck := hc.checkWorkersHealth()
	checks["workers"] = workersCheck

	// Check buffer
	bufferCheck := hc.checkBufferHealth()
	checks["buffer"] = bufferCheck

	// Determine overall status
	overallStatus := "healthy"
	for _, check := range checks {
		if check.Status != "healthy" {
			overallStatus = "unhealthy"
			break
		}
	}

	return &HealthStatus{
		Status:  overallStatus,
		Checks:  checks,
		Uptime:  hc.broker.metrics.Uptime().String(),
		Version: "2.0.0",
	}
}

// checkKafkaHealth checks the health of Kafka connection
func (hc *HealthChecker) checkKafkaHealth() ComponentCheck {
	connected := hc.broker.metrics.KafkaConnected
	status := "healthy"

	if !connected {
		status = "unhealthy"
	}

	var lastMessage *time.Time
	if !hc.broker.metrics.LastMessageTime.IsZero() {
		lastMessage = &hc.broker.metrics.LastMessageTime
	}

	return ComponentCheck{
		Status:      status,
		Connected:   connected,
		LastMessage: lastMessage,
		Details: map[string]interface{}{
			"brokers":       hc.broker.config.KafkaConfig.Brokers,
			"topics":        hc.broker.config.KafkaConfig.Topics,
			"consumerGroup": hc.broker.config.KafkaConfig.ConsumerGroup,
		},
	}
}

// checkMQTTHealth checks the health of MQTT connection
func (hc *HealthChecker) checkMQTTHealth() ComponentCheck {
	connected := hc.broker.metrics.MQTTConnected
	status := "healthy"

	if !connected {
		status = "unhealthy"
	}

	return ComponentCheck{
		Status:    status,
		Connected: connected,
		Details: map[string]interface{}{
			"broker":   hc.broker.config.MQTTConfig.Broker,
			"clientId": hc.broker.config.MQTTConfig.ClientID,
			"qos":      hc.broker.config.MQTTConfig.QoS,
		},
	}
}

// checkWorkersHealth checks the health of message workers
func (hc *HealthChecker) checkWorkersHealth() ComponentCheck {
	active := hc.broker.metrics.ActiveWorkers
	total := hc.broker.config.WorkerCount
	status := "healthy"

	// Consider unhealthy if less than 50% of workers are active
	if active < total/2 {
		status = "unhealthy"
	}

	return ComponentCheck{
		Status: status,
		Active: active,
		Total:  total,
		Details: map[string]interface{}{
			"utilization": float64(active) / float64(total),
		},
	}
}

// checkBufferHealth checks the health of message buffer
func (hc *HealthChecker) checkBufferHealth() ComponentCheck {
	utilization := hc.broker.metrics.BufferUtilization
	size := hc.broker.config.BufferSize
	status := "healthy"

	// Consider unhealthy if buffer utilization is over 90%
	if utilization > 0.9 {
		status = "unhealthy"
	}

	return ComponentCheck{
		Status:      status,
		Utilization: utilization,
		Size:        size,
		Details: map[string]interface{}{
			"utilizationPercent": utilization * 100,
			"capacity":           size,
			"currentSize":        int(float64(size) * utilization),
		},
	}
}

// GetHealthStatus returns the current health status (for testing)
func (hc *HealthChecker) GetHealthStatus() *HealthStatus {
	return hc.performAllHealthChecks()
}

// Legacy method names for backward compatibility with tests
func (hc *HealthChecker) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	health := hc.performAllHealthChecks()

	// Create response in the format expected by tests
	response := HealthCheckResponse{
		Status:  health.Status,
		Uptime:  health.Uptime,
		Version: health.Version,
		Metrics: hc.broker.metrics,
		Checks:  health.Checks,
	}

	// Set HTTP status based on overall health
	statusCode := http.StatusOK
	if health.Status != "healthy" {
		statusCode = http.StatusServiceUnavailable
	}

	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(response)
}

func (hc *HealthChecker) handleMetrics(w http.ResponseWriter, r *http.Request) {
	hc.metricsHandler(w, r)
}

func (hc *HealthChecker) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	health := hc.performAllHealthChecks()

	status := map[string]interface{}{
		"status":  health.Status,
		"checks":  health.Checks,
		"uptime":  health.Uptime,
		"version": health.Version,
		"broker":  "k2m-broker",
		"config": map[string]interface{}{
			"workerCount": hc.broker.config.WorkerCount,
			"bufferSize":  hc.broker.config.BufferSize,
			"routeCount":  len(hc.broker.config.Routes),
		},
		"startTime": hc.broker.metrics.StartTime,
	}

	// Set HTTP status based on overall health
	statusCode := http.StatusOK
	if health.Status != "healthy" {
		statusCode = http.StatusServiceUnavailable
	}

	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(status)
}

// performHealthChecks (legacy method for tests) returns individual checks as a slice
func (hc *HealthChecker) performHealthChecks() []HealthCheck {
	checks := []HealthCheck{}

	// Kafka check
	kafkaConnected := hc.broker.metrics.KafkaConnected
	kafkaStatus := "healthy"
	if !kafkaConnected {
		kafkaStatus = "unhealthy"
	}
	checks = append(checks, HealthCheck{
		Name:   "kafka",
		Status: kafkaStatus,
		Details: map[string]interface{}{
			"connected": kafkaConnected,
		},
	})

	// MQTT check
	mqttConnected := hc.broker.metrics.MQTTConnected
	mqttStatus := "healthy"
	if !mqttConnected {
		mqttStatus = "unhealthy"
	}
	checks = append(checks, HealthCheck{
		Name:   "mqtt",
		Status: mqttStatus,
		Details: map[string]interface{}{
			"connected": mqttConnected,
		},
	})

	// Workers check
	activeWorkers := hc.broker.metrics.ActiveWorkers
	totalWorkers := hc.broker.config.WorkerCount
	workersStatus := "healthy"
	if activeWorkers < totalWorkers/2 {
		workersStatus = "unhealthy"
	}
	checks = append(checks, HealthCheck{
		Name:   "workers",
		Status: workersStatus,
		Details: map[string]interface{}{
			"active": activeWorkers,
			"total":  totalWorkers,
		},
	})

	// Processing check
	checks = append(checks, HealthCheck{
		Name:   "processing",
		Status: "healthy",
		Details: map[string]interface{}{
			"processed": hc.broker.metrics.MessagesProcessed,
			"failed":    hc.broker.metrics.MessagesFailed,
		},
	})

	// Buffer check
	utilization := hc.broker.metrics.BufferUtilization
	bufferStatus := "healthy"
	if utilization > 0.9 {
		bufferStatus = "unhealthy"
	}
	checks = append(checks, HealthCheck{
		Name:   "buffer",
		Status: bufferStatus,
		Details: map[string]interface{}{
			"utilization": utilization,
		},
	})

	// Activity check
	checks = append(checks, HealthCheck{
		Name:   "activity",
		Status: "healthy",
		Details: map[string]interface{}{
			"lastMessage": hc.broker.metrics.LastMessageTime,
		},
	})

	return checks
}

// HealthCheckResponse for backward compatibility with tests
type HealthCheckResponse struct {
	Status  string                    `json:"status"`
	Uptime  string                    `json:"uptime"`
	Version string                    `json:"version"`
	Metrics *Metrics                  `json:"metrics"`
	Checks  map[string]ComponentCheck `json:"checks"`
}

// Individual check structure for iteration
type HealthCheck struct {
	Name    string      `json:"name"`
	Status  string      `json:"status"`
	Details interface{} `json:"details,omitempty"`
}
