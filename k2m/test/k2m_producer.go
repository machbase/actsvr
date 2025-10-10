package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

var stopCh = make(chan struct{})

func main() {
	produceMessages("user-events", 1)
}

func createKafkaProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	return producer, nil
}

func produceMessages(topic string, intervalSeconds int) {
	producer, err := createKafkaProducer()
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			fmt.Println("Stopping message production")
			return
		case <-ticker.C:
			message := map[string]interface{}{
				"device_id": "device123",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"value":     42.5,
			}
			messageBytes, err := json.Marshal(message)
			if err != nil {
				fmt.Println("Failed to marshal message:", err)
				continue
			}

			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(messageBytes),
			})
			if err != nil {
				fmt.Println("Failed to produce message:", err)
			} else {
				fmt.Println("Produced message to topic", topic)
			}
		}
	}
}
