package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "test", "Kafka topic")
	group := flag.String("group", "test-group", "Kafka consumer group")
	flag.Parse()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{*brokers},
		GroupID:  *group,
		Topic:    *topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer r.Close()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("\nInterrotto dall'utente")
		os.Exit(0)
	}()

	fmt.Printf("In ascolto su topic '%s' da broker '%s'...\n", *topic, *brokers)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		m, err := r.ReadMessage(ctx)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Errore nella lettura del messaggio: %v\n", err)
			break
		}
		fmt.Printf("Messaggio ricevuto: %s\n", string(m.Value))
	}
}
