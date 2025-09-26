package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	toolutil "github.com/sandrolain/events-bridge/testers/toolutil"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "rediscli",
		Short: "Redis client tester",
		Long:  "A simple Redis CLI with send and serve commands for channels and streams.",
	}

	// SEND (publisher / stream producer)
	var (
		sendAddr     string
		sendChannel  string
		sendStream   string
		sendGroup    string // unused in send; present for symmetry if needed later
		sendPayload  string
		sendInterval string
		sendDataKey  string
	)
	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Publish periodic messages to a Redis channel or stream",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			rdb := redis.NewClient(&redis.Options{Addr: sendAddr})
			defer func() { _ = rdb.Close() }()

			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			ticker := time.NewTicker(dur)
			defer ticker.Stop()

			mode := "channel"
			if sendStream != "" {
				mode = "stream"
			}
			fmt.Printf("Sending to Redis %s (%s) every %s\n", sendAddr, mode, dur)

			for range ticker.C {
				body, _, err := toolutil.BuildPayload(sendPayload, toolutil.CTText)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				switch mode {
				case "stream":
					fields := map[string]interface{}{sendDataKey: body}
					res := rdb.XAdd(ctx, &redis.XAddArgs{Stream: sendStream, Values: fields})
					if err := res.Err(); err != nil {
						fmt.Fprintf(os.Stderr, "XAdd error: %v\n", err)
					} else {
						fmt.Printf("Message sent to stream %s, id: %s\n", sendStream, res.Val())
					}
				default: // channel
					if err := rdb.Publish(ctx, sendChannel, body).Err(); err != nil {
						fmt.Fprintf(os.Stderr, "Publish error: %v\n", err)
					} else {
						fmt.Printf("Message sent to channel %s (%d bytes)\n", sendChannel, len(body))
					}
				}
			}
			return nil
		},
	}
	sendCmd.Flags().StringVar(&sendAddr, "address", "localhost:6379", "Redis address")
	sendCmd.Flags().StringVar(&sendChannel, "channel", "test", "Redis channel (for pub-sub mode)")
	sendCmd.Flags().StringVar(&sendStream, "stream", "", "Redis stream (if set, sends to stream)")
	sendCmd.Flags().StringVar(&sendGroup, "group", "", "Reserved (no-op for send)")
	sendCmd.Flags().StringVar(&sendDataKey, "dataKey", "data", "Field name holding data in stream messages")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "Hello, Redis!", new(string), "")
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")

	// SERVE (subscriber / stream consumer)
	var (
		subAddr     string
		subChannel  string
		subStream   string
		subGroup    string
		subConsumer string
		subDataKey  string
	)
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Subscribe to a channel or consume a stream and log messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rdb := redis.NewClient(&redis.Options{Addr: subAddr})
			defer func() { _ = rdb.Close() }()

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
			go func() { <-sigc; fmt.Println("\nInterrupted by user"); cancel(); os.Exit(0) }()

			if subStream != "" {
				fmt.Printf("Listening to Redis stream '%s' on %s\n", subStream, subAddr)
				lastID := "$"
				useGroup := subGroup != "" && subConsumer != ""
				if useGroup {
					// Create group idempotently; ignore error if exists
					_ = rdb.XGroupCreateMkStream(ctx, subStream, subGroup, "0").Err()
					lastID = ">"
				}
				for {
					var res []redis.XStream
					var err error
					if useGroup {
						res, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
							Group:    subGroup,
							Consumer: subConsumer,
							Streams:  []string{subStream, lastID},
							Count:    1,
							Block:    0,
							NoAck:    false,
						}).Result()
					} else {
						res, err = rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{subStream, lastID}, Count: 1, Block: 0}).Result()
					}
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error reading from stream: %v\n", err)
						time.Sleep(2 * time.Second)
						continue
					}
					for _, xstream := range res {
						for _, xmsg := range xstream.Messages {
							// Metadata and fields
							var items []toolutil.KV
							items = append(items, toolutil.KV{Key: "ID", Value: xmsg.ID})
							for k, v := range xmsg.Values {
								items = append(items, toolutil.KV{Key: k, Value: fmt.Sprintf("%v", v)})
							}
							sections := []toolutil.MessageSection{
								{Title: "Stream", Items: []toolutil.KV{{Key: "Name", Value: xstream.Stream}}},
								{Title: "Message", Items: items},
							}
							// Extract body
							var data []byte
							if v, ok := xmsg.Values[subDataKey]; ok {
								switch vv := v.(type) {
								case string:
									data = []byte(vv)
								case []byte:
									data = vv
								default:
									data = []byte(fmt.Sprintf("%v", vv))
								}
							}
							ct := toolutil.GuessMIME(data)
							toolutil.PrintColoredMessage("Redis Stream", sections, data, ct)
							if useGroup {
								_ = rdb.XAck(ctx, subStream, subGroup, xmsg.ID).Err()
							} else {
								lastID = xmsg.ID
							}
						}
					}
				}
			}

			// Channel mode
			fmt.Printf("Listening to Redis channel '%s' on %s\n", subChannel, subAddr)
			pubsub := rdb.Subscribe(ctx, subChannel)
			ch := pubsub.Channel()
			for msg := range ch {
				sections := []toolutil.MessageSection{
					{Title: "Channel", Items: []toolutil.KV{{Key: "Name", Value: msg.Channel}}},
				}
				ct := toolutil.GuessMIME([]byte(msg.Payload))
				toolutil.PrintColoredMessage("Redis PubSub", sections, []byte(msg.Payload), ct)
			}
			return nil
		},
	}
	serveCmd.Flags().StringVar(&subAddr, "address", "localhost:6379", "Redis address")
	serveCmd.Flags().StringVar(&subChannel, "channel", "test", "Redis channel (for pub-sub mode)")
	serveCmd.Flags().StringVar(&subStream, "stream", "", "Redis stream (if set, listens to stream)")
	serveCmd.Flags().StringVar(&subGroup, "group", "", "Redis consumer group (stream mode)")
	serveCmd.Flags().StringVar(&subConsumer, "consumer", "", "Redis consumer name (stream mode)")
	serveCmd.Flags().StringVar(&subDataKey, "dataKey", "data", "Field name holding data in stream messages")

	root.AddCommand(sendCmd, serveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
