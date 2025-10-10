package k2m

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestNewMessageRouter(t *testing.T) {
	tests := []struct {
		name        string
		routes      []RouteConfig
		expectError bool
	}{
		{
			name:        "empty routes",
			routes:      []RouteConfig{},
			expectError: false,
		},
		{
			name: "valid routes",
			routes: []RouteConfig{
				{
					Name:     "test_route",
					Priority: 10,
					Filters: []FilterConfig{
						{
							Type: "topic",
							Config: map[string]interface{}{
								"pattern": "test.*",
							},
						},
					},
					Mapping: TopicMapping{
						KafkaTopic: "test",
						MQTTTopic:  "mqtt/test",
						Transform:  "none",
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid filter pattern",
			routes: []RouteConfig{
				{
					Name:     "invalid_route",
					Priority: 10,
					Filters: []FilterConfig{
						{
							Type: "topic",
							Config: map[string]interface{}{
								"pattern": "[invalid",
							},
						},
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router, err := NewMessageRouter(tt.routes)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if router == nil {
				t.Error("expected router but got nil")
			}
		})
	}
}

func TestMessageRouterPriority(t *testing.T) {
	routes := []RouteConfig{
		{
			Name:     "low_priority",
			Priority: 1,
			Filters:  []FilterConfig{},
			Mapping: TopicMapping{
				KafkaTopic: "test",
				MQTTTopic:  "mqtt/low",
				Transform:  "none",
			},
		},
		{
			Name:     "high_priority",
			Priority: 10,
			Filters:  []FilterConfig{},
			Mapping: TopicMapping{
				KafkaTopic: "test",
				MQTTTopic:  "mqtt/high",
				Transform:  "none",
			},
		},
	}

	router, err := NewMessageRouter(routes)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	message := &sarama.ConsumerMessage{
		Topic: "test",
		Value: []byte("test message"),
	}

	route := router.FindRoute(message)
	if route == nil {
		t.Fatal("expected route but got nil")
	}

	// Should match high priority route first
	if route.Name != "high_priority" {
		t.Errorf("expected high_priority route, got %s", route.Name)
	}
}

func TestHeaderFilter(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		message     *sarama.ConsumerMessage
		shouldMatch bool
	}{
		{
			name: "matching header",
			config: map[string]interface{}{
				"key":     "content-type",
				"pattern": "application/json",
			},
			message: &sarama.ConsumerMessage{
				Headers: []*sarama.RecordHeader{
					{Key: []byte("content-type"), Value: []byte("application/json")},
				},
			},
			shouldMatch: true,
		},
		{
			name: "non-matching header",
			config: map[string]interface{}{
				"key":     "content-type",
				"pattern": "application/xml",
			},
			message: &sarama.ConsumerMessage{
				Headers: []*sarama.RecordHeader{
					{Key: []byte("content-type"), Value: []byte("application/json")},
				},
			},
			shouldMatch: false,
		},
		{
			name: "missing optional header",
			config: map[string]interface{}{
				"key":      "optional-header",
				"pattern":  ".*",
				"required": false,
			},
			message: &sarama.ConsumerMessage{
				Headers: []*sarama.RecordHeader{},
			},
			shouldMatch: true,
		},
		{
			name: "missing required header",
			config: map[string]interface{}{
				"key":      "required-header",
				"pattern":  ".*",
				"required": true,
			},
			message: &sarama.ConsumerMessage{
				Headers: []*sarama.RecordHeader{},
			},
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := createHeaderFilter(tt.config)
			if err != nil {
				t.Fatalf("failed to create header filter: %v", err)
			}

			result := filter.ShouldProcess(tt.message)
			if result != tt.shouldMatch {
				t.Errorf("expected %v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestKeyFilter(t *testing.T) {
	config := map[string]interface{}{
		"pattern": "user_.*",
	}

	filter, err := createKeyFilter(config)
	if err != nil {
		t.Fatalf("failed to create key filter: %v", err)
	}

	tests := []struct {
		name        string
		messageKey  []byte
		shouldMatch bool
	}{
		{"matching key", []byte("user_123"), true},
		{"non-matching key", []byte("order_456"), false},
		{"empty key", []byte(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := &sarama.ConsumerMessage{Key: tt.messageKey}
			result := filter.ShouldProcess(message)
			if result != tt.shouldMatch {
				t.Errorf("expected %v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestValueFilter(t *testing.T) {
	tests := []struct {
		name         string
		config       map[string]interface{}
		messageValue []byte
		shouldMatch  bool
	}{
		{
			name: "simple pattern match",
			config: map[string]interface{}{
				"pattern": "error",
			},
			messageValue: []byte("This is an error message"),
			shouldMatch:  true,
		},
		{
			name: "JSON path extraction",
			config: map[string]interface{}{
				"pattern":  "CRITICAL",
				"jsonPath": "level",
			},
			messageValue: []byte(`{"level": "CRITICAL", "message": "System failure"}`),
			shouldMatch:  true,
		},
		{
			name: "JSON path no match",
			config: map[string]interface{}{
				"pattern":  "DEBUG",
				"jsonPath": "level",
			},
			messageValue: []byte(`{"level": "CRITICAL", "message": "System failure"}`),
			shouldMatch:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := createValueFilter(tt.config)
			if err != nil {
				t.Fatalf("failed to create value filter: %v", err)
			}

			message := &sarama.ConsumerMessage{Value: tt.messageValue}
			result := filter.ShouldProcess(message)
			if result != tt.shouldMatch {
				t.Errorf("expected %v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestTopicFilter(t *testing.T) {
	config := map[string]interface{}{
		"pattern": "logs\\..*",
	}

	filter, err := createTopicFilter(config)
	if err != nil {
		t.Fatalf("failed to create topic filter: %v", err)
	}

	tests := []struct {
		name        string
		topic       string
		shouldMatch bool
	}{
		{"matching topic", "logs.application", true},
		{"non-matching topic", "metrics.cpu", false},
		{"partial match", "application.logs", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := &sarama.ConsumerMessage{Topic: tt.topic}
			result := filter.ShouldProcess(message)
			if result != tt.shouldMatch {
				t.Errorf("expected %v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestSizeFilter(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		messageSize int
		shouldMatch bool
	}{
		{
			name: "within range",
			config: map[string]interface{}{
				"min": 10,
				"max": 100,
			},
			messageSize: 50,
			shouldMatch: true,
		},
		{
			name: "below minimum",
			config: map[string]interface{}{
				"min": 10,
				"max": 100,
			},
			messageSize: 5,
			shouldMatch: false,
		},
		{
			name: "above maximum",
			config: map[string]interface{}{
				"min": 10,
				"max": 100,
			},
			messageSize: 150,
			shouldMatch: false,
		},
		{
			name: "exactly minimum",
			config: map[string]interface{}{
				"min": 10,
			},
			messageSize: 10,
			shouldMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := createSizeFilter(tt.config)
			if err != nil {
				t.Fatalf("failed to create size filter: %v", err)
			}

			message := &sarama.ConsumerMessage{
				Value: make([]byte, tt.messageSize),
			}
			result := filter.ShouldProcess(message)
			if result != tt.shouldMatch {
				t.Errorf("expected %v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestTimestampFilter(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name        string
		config      map[string]interface{}
		timestamp   time.Time
		shouldMatch bool
	}{
		{
			name: "recent message",
			config: map[string]interface{}{
				"maxAge": int64(3600), // 1 hour
			},
			timestamp:   now.Add(-30 * time.Minute),
			shouldMatch: true,
		},
		{
			name: "old message",
			config: map[string]interface{}{
				"maxAge": int64(3600), // 1 hour
			},
			timestamp:   now.Add(-2 * time.Hour),
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := createTimestampFilter(tt.config)
			if err != nil {
				t.Fatalf("failed to create timestamp filter: %v", err)
			}

			message := &sarama.ConsumerMessage{
				Timestamp: tt.timestamp,
			}
			result := filter.ShouldProcess(message)
			if result != tt.shouldMatch {
				t.Errorf("expected %v, got %v", tt.shouldMatch, result)
			}
		})
	}
}

func TestComplexRouting(t *testing.T) {
	routes := []RouteConfig{
		{
			Name:     "critical_logs",
			Priority: 100,
			Filters: []FilterConfig{
				{
					Type: "topic",
					Config: map[string]interface{}{
						"pattern": "logs\\..*",
					},
				},
				{
					Type: "value",
					Config: map[string]interface{}{
						"pattern":  "CRITICAL|ERROR",
						"jsonPath": "level",
					},
				},
			},
			Mapping: TopicMapping{
				KafkaTopic: "logs.application",
				MQTTTopic:  "alerts/critical",
				Transform:  "none",
			},
		},
		{
			Name:     "all_logs",
			Priority: 50,
			Filters: []FilterConfig{
				{
					Type: "topic",
					Config: map[string]interface{}{
						"pattern": "logs\\..*",
					},
				},
			},
			Mapping: TopicMapping{
				KafkaTopic: "logs.application",
				MQTTTopic:  "logs/application",
				Transform:  "none",
			},
		},
		{
			Name:     "default",
			Priority: 1,
			Filters:  []FilterConfig{},
			Mapping: TopicMapping{
				KafkaTopic: "{kafkaTopic}",
				MQTTTopic:  "mqtt/{kafkaTopic}",
				Transform:  "none",
			},
		},
	}

	router, err := NewMessageRouter(routes)
	if err != nil {
		t.Fatalf("failed to create router: %v", err)
	}

	tests := []struct {
		name          string
		message       *sarama.ConsumerMessage
		expectedRoute string
	}{
		{
			name: "critical log message",
			message: &sarama.ConsumerMessage{
				Topic: "logs.application",
				Value: []byte(`{"level": "CRITICAL", "message": "System down"}`),
			},
			expectedRoute: "critical_logs",
		},
		{
			name: "info log message",
			message: &sarama.ConsumerMessage{
				Topic: "logs.application",
				Value: []byte(`{"level": "INFO", "message": "User login"}`),
			},
			expectedRoute: "all_logs",
		},
		{
			name: "non-log message",
			message: &sarama.ConsumerMessage{
				Topic: "metrics.cpu",
				Value: []byte(`{"cpu": 85.2}`),
			},
			expectedRoute: "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := router.FindRoute(tt.message)
			if route == nil {
				t.Fatal("expected route but got nil")
			}
			if route.Name != tt.expectedRoute {
				t.Errorf("expected route %s, got %s", tt.expectedRoute, route.Name)
			}
		})
	}
}

func TestValidateRouteConfig(t *testing.T) {
	tests := []struct {
		name        string
		routes      []RouteConfig
		expectError bool
		errorMsg    string
	}{
		{
			name:        "empty routes",
			routes:      []RouteConfig{},
			expectError: true,
			errorMsg:    "at least one route must be configured",
		},
		{
			name: "valid routes",
			routes: []RouteConfig{
				{
					Name:    "test",
					Filters: []FilterConfig{},
					Mapping: TopicMapping{
						KafkaTopic: "test",
						MQTTTopic:  "mqtt/test",
					},
				},
			},
			expectError: false,
		},
		{
			name: "duplicate names",
			routes: []RouteConfig{
				{
					Name: "duplicate",
					Mapping: TopicMapping{
						KafkaTopic: "test1",
						MQTTTopic:  "mqtt/test1",
					},
				},
				{
					Name: "duplicate",
					Mapping: TopicMapping{
						KafkaTopic: "test2",
						MQTTTopic:  "mqtt/test2",
					},
				},
			},
			expectError: true,
			errorMsg:    "duplicate route name",
		},
		{
			name: "empty kafka topic",
			routes: []RouteConfig{
				{
					Name: "invalid",
					Mapping: TopicMapping{
						KafkaTopic: "",
						MQTTTopic:  "mqtt/test",
					},
				},
			},
			expectError: true,
			errorMsg:    "kafkaTopic cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRouteConfig(tt.routes)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
					return
				}
				if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestDefaultRouteConfig(t *testing.T) {
	routes := DefaultRouteConfig()
	if len(routes) != 1 {
		t.Errorf("expected 1 default route, got %d", len(routes))
	}

	defaultRoute := routes[0]
	if defaultRoute.Name != "default" {
		t.Errorf("expected default route name, got %s", defaultRoute.Name)
	}

	if len(defaultRoute.Filters) != 0 {
		t.Errorf("expected no filters for default route, got %d", len(defaultRoute.Filters))
	}

	if err := ValidateRouteConfig(routes); err != nil {
		t.Errorf("default route config should be valid: %v", err)
	}
}

// Helper function for string containment check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				indexOfSubstring(s, substr) >= 0)))
}

func indexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
