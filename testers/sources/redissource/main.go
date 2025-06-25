package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"context"

	"github.com/go-redis/redis/v8"
	testpayload "github.com/sandrolain/events-bridge/testers/sources/testpayload"
)

func main() {
	address := flag.String("address", "localhost:6379", "Redis server address:port")
	channel := flag.String("channel", "test-channel", "Redis channel name")
	stream := flag.String("stream", "", "Redis stream name (if set, sends to stream instead of channel)")
	dataKey := flag.String("dataKey", "data", "Field name for the message in the stream")
	payload := flag.String("payload", "Hello, Redis!", "Message to send")
	interval := flag.String("interval", "5s", "Send interval (duration, e.g. 5s, 1m)")
	testPayloadType := flag.String("testpayload", "", "If set, use testpayload generator: json, cbor, sentiment, sentence")
	flag.Parse()

	dur, err := time.ParseDuration(*interval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid interval duration: %v\n", err)
		os.Exit(1)
	}

	opt := &redis.Options{
		Addr: *address,
	}
	rdb := redis.NewClient(opt)
	ctx := context.Background()
	defer rdb.Close()

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

	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	if *stream != "" {
		fmt.Printf("Connected to Redis %s, stream: %s\n", *address, *stream)
		for range ticker.C {
			b, err := getPayload()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Payload error: %v\n", err)
				continue
			}
			fields := map[string]interface{}{*dataKey: b}
			res := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: *stream,
				Values: fields,
			})
			if res.Err() != nil {
				fmt.Fprintf(os.Stderr, "XAdd error: %v\n", res.Err())
			} else {
				fmt.Printf("Message sent to stream %s, id: %s\n", *stream, res.Val())
			}
		}
	} else {
		fmt.Printf("Connected to Redis %s, channel: %s\n", *address, *channel)
		for range ticker.C {
			b, err := getPayload()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Payload error: %v\n", err)
				continue
			}
			res := rdb.Publish(ctx, *channel, b)
			if res.Err() != nil {
				fmt.Fprintf(os.Stderr, "Publish error: %v\n", res.Err())
			} else {
				fmt.Println("Message successfully sent to Redis!")
			}
		}
	}
}
