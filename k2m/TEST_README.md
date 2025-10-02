# K2M Broker Unit Tests

This directory contains comprehensive unit tests for the K2M Broker implementation.

## Test Files

### `k2m_test.go`
- **Configuration Tests**: Tests for `K2MConfig`, `KafkaConfig`, and `MQTTConfig` structures
- **Broker Lifecycle Tests**: Tests for broker creation, initialization, and context management
- **Message Transformation Tests**: Tests for different transformation modes (none, json)
- **Topic Mapping Tests**: Tests for topic templating with `{kafkaTopic}`, `{partition}`, `{key}`
- **Performance Benchmarks**: Benchmark tests for message processing performance

### `k2m_integration_test.go`
- **Mock Implementations**: Complete mock implementations for MQTT client and Kafka consumer
- **Integration Tests**: End-to-end message flow testing
- **Broker Lifecycle Tests**: Tests for Start/Stop functionality with mocked dependencies
- **Worker Processing Tests**: Tests for message worker functionality
- **Error Handling Tests**: Tests for various error scenarios

### `cmd/k2mbroker/main_test.go`
- **CLI Configuration Tests**: Tests for command-line argument parsing
- **File Configuration Tests**: Tests for JSON configuration file loading
- **Topic Mapping Parser Tests**: Tests for topic mapping string parsing
- **PID File Management Tests**: Tests for PID file creation and cleanup
- **Error Handling Tests**: Tests for invalid configurations and file errors

## Test Coverage

The tests cover:

1. **Configuration Management**
   - Default configuration creation
   - JSON serialization/deserialization
   - Command-line flag parsing
   - Configuration file loading
   - Error handling for invalid configurations

2. **Broker Functionality**
   - Broker creation and initialization
   - Context management and cancellation
   - Message processing pipeline
   - Worker management
   - Graceful shutdown

3. **Message Processing**
   - Message transformation (none, json)
   - Topic mapping with templating
   - MQTT publishing
   - Error handling for message processing

4. **Integration Scenarios**
   - Complete message flow from Kafka to MQTT
   - Multi-worker message processing
   - Connection error handling
   - Graceful shutdown scenarios

5. **Performance**
   - Message transformation benchmarks
   - Topic resolution benchmarks
   - Configuration parsing benchmarks

## Running Tests

### Run all tests:
```bash
go test ./k2m ./cmd/k2mbroker -v
```

### Run tests with coverage:
```bash
go test ./k2m ./cmd/k2mbroker -v -cover
```

### Run specific test categories:
```bash
# Configuration tests only
go test ./k2m -run TestK2MConfig -v

# Message processing tests only
go test ./k2m -run TestMessageWorker -v

# CLI tests only
go test ./cmd/k2mbroker -run TestParse -v
```

### Run benchmark tests:
```bash
go test ./k2m -bench=. -benchmem
```

### Run integration tests:
```bash
go test ./k2m -run Integration -v
```

## Mock Components

The test suite includes comprehensive mocks for external dependencies:

- **MockMQTTClient**: Full MQTT client mock with message tracking
- **MockSaramaConsumerGroup**: Kafka consumer group mock with session simulation
- **MockConsumerGroupSession**: Consumer session mock with claim management
- **MockConsumerGroupClaim**: Message claim mock with configurable message injection

These mocks allow for:
- Isolated unit testing without external dependencies
- Simulation of error conditions
- Message flow verification
- Performance testing without network overhead

## Test Data

Tests use realistic test data including:
- Sample Kafka messages with various key/value combinations
- Multiple topic mapping configurations
- Various configuration scenarios (development, production)
- Error scenarios (network failures, invalid configurations)

## Continuous Integration

These tests are designed to run in CI/CD environments and include:
- No external dependency requirements
- Deterministic test outcomes
- Fast execution times
- Comprehensive error scenario coverage
- Memory leak detection through proper cleanup