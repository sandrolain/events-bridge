package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
)

func main() {
	projectID := flag.String("project", "test-project", "Google Cloud Project ID")
	subscriptionID := flag.String("subscription", "test-subscription", "Pub/Sub subscription ID")
	flag.Parse()

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Pub/Sub client error: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	sub := client.Subscription(*subscriptionID)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("\nInterrupted by user")
		os.Exit(0)
	}()

	fmt.Printf("Listening to Pub/Sub project %s, subscription: %s\n", *projectID, *subscriptionID)
	for {
		err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			timestamp := time.Now().Format(time.RFC3339)
			fmt.Printf("[%s] Received message: %s\n", timestamp, string(m.Data))
			m.Ack()
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Receive error: %v\n", err)
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
}
