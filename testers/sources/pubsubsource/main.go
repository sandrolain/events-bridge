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
	testpayload "github.com/sandrolain/events-bridge/testers/sources/testpayload"
)

func main() {
	projectID := flag.String("project", "test-project", "Google Cloud Project ID")
	topicID := flag.String("topic", "test-topic", "Pub/Sub topic ID")
	payload := flag.String("payload", "Hello, PubSub!", "Message to send")
	interval := flag.String("interval", "5s", "Send interval (duration, e.g. 5s, 1m)")
	testPayloadType := flag.String("testpayload", "", "If set, use testpayload generator: json, cbor, sentiment, sentence")
	flag.Parse()

	dur, err := time.ParseDuration(*interval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid interval duration: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, *projectID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Pub/Sub client error: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to close Pub/Sub client: %v\n", err)
		}
	}()
	topic := client.Topic(*topicID)
	defer topic.Stop()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("\nInterrupted by user")
		os.Exit(0)
	}()

	getPayload := func() ([]byte, error) {
		if *testPayloadType != "" {
			switch *testPayloadType {
			case "json":
				return testpayload.GenerateRandomJSON()
			case "cbor":
				return testpayload.GenerateRandomCBOR()
			case "sentiment":
				return []byte(testpayload.GenerateSentimentPhrase()), nil
			case "sentence":
				return []byte(testpayload.GenerateSentence()), nil
			default:
				return nil, fmt.Errorf("unknown testpayload type: %s", *testPayloadType)
			}
		} else {
			return testpayload.Interpolate(*payload)
		}
	}

	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	fmt.Printf("Connected to Pub/Sub project %s, topic: %s\n", *projectID, *topicID)
	for range ticker.C {
		b, err := getPayload()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Payload error: %v\n", err)
			continue
		}
		result := topic.Publish(ctx, &pubsub.Message{Data: b})
		id, err := result.Get(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending message: %v\n", err)
		} else {
			fmt.Printf("Message successfully sent to Pub/Sub! ID: %s\n", id)
		}
	}
}
