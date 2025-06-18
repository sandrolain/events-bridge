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
		return fmt.Errorf("errore di connessione a Kafka: %w", err)
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	})
	if err != nil {
		return fmt.Errorf("errore nella creazione del topic: %w", err)
	}
	return nil
}
