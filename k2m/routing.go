package k2m

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

// MessageFilter defines the interface for message filtering
type MessageFilter interface {
	// ShouldProcess returns true if the message should be processed
	ShouldProcess(message *sarama.ConsumerMessage) bool
	// GetName returns the filter name for logging
	GetName() string
}

// FilterConfig holds configuration for message filtering
type FilterConfig struct {
	Type   string                 `json:"type"`   // "header", "key", "value", "topic", "custom"
	Config map[string]interface{} `json:"config"` // Filter-specific configuration
}

// RouteConfig defines routing rules for messages
type RouteConfig struct {
	Name     string         `json:"name"`     // Route name for identification
	Filters  []FilterConfig `json:"filters"`  // Filters that must match
	Mapping  TopicMapping   `json:"mapping"`  // Topic mapping for matched messages
	Priority int            `json:"priority"` // Higher priority routes are checked first
}

// MessageRouter handles message routing based on filters
type MessageRouter struct {
	routes  []RouteConfig
	filters map[string]MessageFilter
}

// NewMessageRouter creates a new message router
func NewMessageRouter(routes []RouteConfig) (*MessageRouter, error) {
	router := &MessageRouter{
		routes:  routes,
		filters: make(map[string]MessageFilter),
	}

	// Sort routes by priority (higher first)
	for i := 0; i < len(router.routes)-1; i++ {
		for j := i + 1; j < len(router.routes); j++ {
			if router.routes[i].Priority < router.routes[j].Priority {
				router.routes[i], router.routes[j] = router.routes[j], router.routes[i]
			}
		}
	}

	// Initialize filters for all routes
	for _, route := range router.routes {
		for _, filterConfig := range route.Filters {
			filterKey := fmt.Sprintf("%s_%s", route.Name, filterConfig.Type)
			filter, err := createFilter(filterConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create filter for route %s: %w", route.Name, err)
			}
			router.filters[filterKey] = filter
		}
	}

	return router, nil
}

// FindRoute finds the first matching route for a message
func (mr *MessageRouter) FindRoute(message *sarama.ConsumerMessage) *RouteConfig {
	for _, route := range mr.routes {
		if mr.routeMatches(message, &route) {
			return &route
		}
	}
	return nil
}

// routeMatches checks if a message matches all filters for a route
func (mr *MessageRouter) routeMatches(message *sarama.ConsumerMessage, route *RouteConfig) bool {
	for _, filterConfig := range route.Filters {
		filterKey := fmt.Sprintf("%s_%s", route.Name, filterConfig.Type)
		filter, exists := mr.filters[filterKey]
		if !exists {
			return false
		}
		if !filter.ShouldProcess(message) {
			return false
		}
	}
	return true
}

// createFilter creates a filter based on configuration
func createFilter(config FilterConfig) (MessageFilter, error) {
	switch config.Type {
	case "header":
		return createHeaderFilter(config.Config)
	case "key":
		return createKeyFilter(config.Config)
	case "value":
		return createValueFilter(config.Config)
	case "topic":
		return createTopicFilter(config.Config)
	case "size":
		return createSizeFilter(config.Config)
	case "timestamp":
		return createTimestampFilter(config.Config)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", config.Type)
	}
}

// HeaderFilter filters messages based on Kafka headers
type HeaderFilter struct {
	name      string
	headerKey string
	pattern   *regexp.Regexp
	required  bool
}

func createHeaderFilter(config map[string]interface{}) (*HeaderFilter, error) {
	headerKey, ok := config["key"].(string)
	if !ok {
		return nil, fmt.Errorf("header filter requires 'key' field")
	}

	patternStr, ok := config["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("header filter requires 'pattern' field")
	}

	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern in header filter: %w", err)
	}

	required := false
	if r, ok := config["required"].(bool); ok {
		required = r
	}

	return &HeaderFilter{
		name:      fmt.Sprintf("header_%s", headerKey),
		headerKey: headerKey,
		pattern:   pattern,
		required:  required,
	}, nil
}

func (hf *HeaderFilter) ShouldProcess(message *sarama.ConsumerMessage) bool {
	for _, header := range message.Headers {
		if string(header.Key) == hf.headerKey {
			return hf.pattern.Match(header.Value)
		}
	}
	return !hf.required // If header not found, allow only if not required
}

func (hf *HeaderFilter) GetName() string {
	return hf.name
}

// KeyFilter filters messages based on message key
type KeyFilter struct {
	name    string
	pattern *regexp.Regexp
}

func createKeyFilter(config map[string]interface{}) (*KeyFilter, error) {
	patternStr, ok := config["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("key filter requires 'pattern' field")
	}

	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern in key filter: %w", err)
	}

	return &KeyFilter{
		name:    "key_filter",
		pattern: pattern,
	}, nil
}

func (kf *KeyFilter) ShouldProcess(message *sarama.ConsumerMessage) bool {
	return kf.pattern.Match(message.Key)
}

func (kf *KeyFilter) GetName() string {
	return kf.name
}

// ValueFilter filters messages based on message value content
type ValueFilter struct {
	name     string
	pattern  *regexp.Regexp
	jsonPath string // For JSON content filtering
}

func createValueFilter(config map[string]interface{}) (*ValueFilter, error) {
	patternStr, ok := config["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("value filter requires 'pattern' field")
	}

	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern in value filter: %w", err)
	}

	jsonPath := ""
	if jp, ok := config["jsonPath"].(string); ok {
		jsonPath = jp
	}

	return &ValueFilter{
		name:     "value_filter",
		pattern:  pattern,
		jsonPath: jsonPath,
	}, nil
}

func (vf *ValueFilter) ShouldProcess(message *sarama.ConsumerMessage) bool {
	content := string(message.Value)

	// If JSON path is specified, extract that field
	if vf.jsonPath != "" {
		var data map[string]interface{}
		if err := json.Unmarshal(message.Value, &data); err == nil {
			if value, exists := data[vf.jsonPath]; exists {
				content = fmt.Sprintf("%v", value)
			}
		}
	}

	return vf.pattern.MatchString(content)
}

func (vf *ValueFilter) GetName() string {
	return vf.name
}

// TopicFilter filters messages based on topic name
type TopicFilter struct {
	name    string
	pattern *regexp.Regexp
}

func createTopicFilter(config map[string]interface{}) (*TopicFilter, error) {
	patternStr, ok := config["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("topic filter requires 'pattern' field")
	}

	pattern, err := regexp.Compile(patternStr)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern in topic filter: %w", err)
	}

	return &TopicFilter{
		name:    "topic_filter",
		pattern: pattern,
	}, nil
}

func (tf *TopicFilter) ShouldProcess(message *sarama.ConsumerMessage) bool {
	return tf.pattern.MatchString(message.Topic)
}

func (tf *TopicFilter) GetName() string {
	return tf.name
}

// SizeFilter filters messages based on payload size
type SizeFilter struct {
	name    string
	minSize int
	maxSize int
}

func createSizeFilter(config map[string]interface{}) (*SizeFilter, error) {
	minSize := 0
	maxSize := int(^uint(0) >> 1) // Max int

	if min, ok := config["min"]; ok {
		switch v := min.(type) {
		case float64:
			minSize = int(v)
		case int:
			minSize = v
		case string:
			if parsed, err := strconv.Atoi(v); err == nil {
				minSize = parsed
			}
		}
	}

	if max, ok := config["max"]; ok {
		switch v := max.(type) {
		case float64:
			maxSize = int(v)
		case int:
			maxSize = v
		case string:
			if parsed, err := strconv.Atoi(v); err == nil {
				maxSize = parsed
			}
		}
	}

	return &SizeFilter{
		name:    "size_filter",
		minSize: minSize,
		maxSize: maxSize,
	}, nil
}

func (sf *SizeFilter) ShouldProcess(message *sarama.ConsumerMessage) bool {
	size := len(message.Value)
	return size >= sf.minSize && size <= sf.maxSize
}

func (sf *SizeFilter) GetName() string {
	return sf.name
}

// TimestampFilter filters messages based on timestamp
type TimestampFilter struct {
	name   string
	minAge int64 // Minimum age in seconds
	maxAge int64 // Maximum age in seconds
}

func createTimestampFilter(config map[string]interface{}) (*TimestampFilter, error) {
	minAge := int64(0)
	maxAge := int64(86400 * 365) // 1 year in seconds

	if min, ok := config["minAge"]; ok {
		switch v := min.(type) {
		case float64:
			minAge = int64(v)
		case int64:
			minAge = v
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				minAge = parsed
			}
		}
	}

	if max, ok := config["maxAge"]; ok {
		switch v := max.(type) {
		case float64:
			maxAge = int64(v)
		case int64:
			maxAge = v
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				maxAge = parsed
			}
		}
	}

	return &TimestampFilter{
		name:   "timestamp_filter",
		minAge: minAge,
		maxAge: maxAge,
	}, nil
}

func (tf *TimestampFilter) ShouldProcess(message *sarama.ConsumerMessage) bool {
	if message.Timestamp.IsZero() {
		return true // Allow messages without timestamp
	}

	ageSeconds := int64(time.Since(message.Timestamp).Seconds())
	return ageSeconds >= tf.minAge && ageSeconds <= tf.maxAge
}

func (tf *TimestampFilter) GetName() string {
	return tf.name
}

// DefaultRouteConfig creates a default route configuration
func DefaultRouteConfig() []RouteConfig {
	return []RouteConfig{
		{
			Name:     "default",
			Filters:  []FilterConfig{}, // No filters = match all
			Priority: 0,
			Mapping: TopicMapping{
				KafkaTopic: "{kafkaTopic}",
				MQTTTopic:  "mqtt/{kafkaTopic}",
				Transform:  "none",
			},
		},
	}
}

// ValidateRouteConfig validates a route configuration
func ValidateRouteConfig(routes []RouteConfig) error {
	if len(routes) == 0 {
		return fmt.Errorf("at least one route must be configured")
	}

	routeNames := make(map[string]bool)
	for _, route := range routes {
		if route.Name == "" {
			return fmt.Errorf("route name cannot be empty")
		}
		if routeNames[route.Name] {
			return fmt.Errorf("duplicate route name: %s", route.Name)
		}
		routeNames[route.Name] = true

		if route.Mapping.KafkaTopic == "" {
			return fmt.Errorf("route %s: kafkaTopic cannot be empty", route.Name)
		}
		if route.Mapping.MQTTTopic == "" {
			return fmt.Errorf("route %s: mqttTopic cannot be empty", route.Name)
		}

		// Validate filters
		for i, filter := range route.Filters {
			if filter.Type == "" {
				return fmt.Errorf("route %s: filter %d type cannot be empty", route.Name, i)
			}
			if filter.Config == nil {
				return fmt.Errorf("route %s: filter %d config cannot be nil", route.Name, i)
			}
		}
	}

	return nil
}
