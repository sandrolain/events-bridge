package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	testpayload "github.com/sandrolain/events-bridge/testers/sources/testpayload"
	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "test", "Kafka topic")
	payload := flag.String("payload", "Hello, Kafka!", "Message to send")
	interval := flag.String("interval", "5s", "Send interval (duration, es: 5s, 1m)")
	testPayloadType := flag.String("testpayload", "", "If set, use testpayload generator: json, cbor, sentiment, sentence")
	flag.Parse()

	dur, err := time.ParseDuration(*interval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid interval duration: %v\n", err)
		os.Exit(1)
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{*brokers},
		Topic:   *topic,
	})
	defer func() {
		if err := writer.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to close Kafka writer: %v\n", err)
		}
	}()

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
				return nil, fmt.Errorf("unknown test payload type: %s", *testPayloadType)
			}
		} else {
			return testpayload.Interpolate(*payload)
		}
	}

	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	fmt.Printf("Connected to Kafka %s, topic: %s\n", *brokers, *topic)
	for range ticker.C {
		b, err := getPayload()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Payload error: %v\n", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = writer.WriteMessages(ctx, kafka.Message{
			Value: b,
		})
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error sending message: %v\n", err)
		} else {
			fmt.Println("Message successfully sent to Kafka!")
		}
	}
}
