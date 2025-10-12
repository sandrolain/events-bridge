package main

import (
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

func ensureKafkaTopic(s *slog.Logger, brokers []string, topic string, partitions int, replicationFactor int) error {
	s.Info("ensuring Kafka topic exists", "brokers", brokers, "topic", topic, "partitions", partitions, "replicationFactor", replicationFactor)
	if partitions <= 0 {
		partitions = 1
	}
	if replicationFactor <= 0 {
		replicationFactor = 1
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("error connecting to Kafka: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			s.Error("error closing Kafka connection", "err", err)
		}
	}()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return fmt.Errorf("error creating Kafka topic: %w", err)
	}
	return nil
}

// ensureKafkaTopicWithDialer creates a Kafka topic using a custom dialer with TLS/SASL support.
func ensureKafkaTopicWithDialer(s *slog.Logger, dialer *kafka.Dialer, brokers []string, topic string, partitions int, replicationFactor int) error {
	s.Info("ensuring Kafka topic exists", "brokers", brokers, "topic", topic, "partitions", partitions, "replicationFactor", replicationFactor)
	if partitions <= 0 {
		partitions = 1
	}
	if replicationFactor <= 0 {
		replicationFactor = 1
	}

	conn, err := dialer.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("error connecting to Kafka: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			s.Error("error closing Kafka connection", "err", err)
		}
	}()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return fmt.Errorf("error creating Kafka topic: %w", err)
	}
	return nil
}
