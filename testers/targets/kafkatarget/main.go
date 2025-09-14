package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "test", "Kafka topic")
	group := flag.String("group", "", "Kafka consumer group")
	flag.Parse()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{*brokers},
		GroupID:  *group,
		Topic:    *topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer func() {
		if err := r.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to close Kafka reader: %v\n", err)
		}
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("\nInterrupted by user")
		os.Exit(0)
	}()

	fmt.Printf("Listening on topic '%s' from broker '%s'...\n", *topic, *brokers)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading message: %v\n", err)
			break
		}
		fmt.Printf("Message received: %s\n", string(m.Value))
	}
}
