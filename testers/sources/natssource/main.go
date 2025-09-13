package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	testpayload "github.com/sandrolain/events-bridge/testers/sources/testpayload"
)

func main() {
	address := flag.String("address", "nats://localhost:4222", "NATS server address")
	subject := flag.String("subject", "test.subject", "NATS subject")
	payload := flag.String("payload", "{nowtime}", "Payload to send")
	interval := flag.String("interval", "5s", "Send interval (duration, es: 5s, 1m)")
	stream := flag.String("stream", "", "JetStream stream name (if set, uses JetStream)")
	testPayloadType := flag.String("testpayload", "", "If set, use testpayload generator: json, cbor, sentiment, sentence")
	flag.Parse()

	dur, err := time.ParseDuration(*interval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid interval duration: %v\n", err)
		os.Exit(1)
	}

	nc, err := nats.Connect(*address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NATS connection error: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

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
				return nil, fmt.Errorf("Unknown testpayload type: %s", *testPayloadType)
			}
		} else {
			return testpayload.Interpolate(*payload)
		}
	}

	if *stream != "" {
		js, err := nc.JetStream()
		if err != nil {
			fmt.Fprintf(os.Stderr, "JetStream context error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Connected to %s, subject: %s (JetStream mode), stream: %s\n", *address, *subject, *stream)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for range ticker.C {
			b, err := getPayload()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Payload error: %v\n", err)
				continue
			}
			ack, err := js.Publish(*subject, b)
			if err != nil {
				fmt.Fprintf(os.Stderr, "JetStream publish error: %v\n", err)
			} else {
				fmt.Printf("Payload sent to %s (JetStream), seq: %d\n", *subject, ack.Sequence)
			}
		}
	} else {
		fmt.Printf("Connected to %s, subject: %s\n", *address, *subject)
		ticker := time.NewTicker(dur)
		defer ticker.Stop()
		for range ticker.C {
			b, err := getPayload()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Payload error: %v\n", err)
				continue
			}
			err = nc.Publish(*subject, b)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Publish error: %v\n", err)
			} else {
				fmt.Printf("Payload sent to %s: %s\n", *subject, string(b))
			}
		}
	}
}
