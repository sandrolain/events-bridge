package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/redis/go-redis/v9"
)

func printMsgDetails(mode, key string, data []byte, meta map[string]interface{}) {
	black := color.New(color.FgBlack).Add(color.ResetUnderline).PrintfFunc()
	blue := color.New(color.FgHiBlue).Add(color.Underline).PrintfFunc()
	white := color.New(color.FgWhite).Add(color.ResetUnderline).PrintfFunc()

	black("\n----------------------------------------\n")
	black(time.Now().Format(time.RFC3339) + "\n")
	blue("Mode: ")
	white("%s\n", mode)
	blue("Key: ")
	white("%s\n", key)
	if len(meta) > 0 {
		blue("Metadata:\n")
		for k, v := range meta {
			white("  %s: %v\n", k, v)
		}
	}
	blue("Payload:\n")
	if len(data) > 0 {
		white("%s\n\n", string(data))
	} else {
		white("<empty>\n\n")
	}
}

func main() {
	address := flag.String("address", "localhost:6379", "Redis address")
	channel := flag.String("channel", "test", "Redis channel (for pub-sub mode)")
	stream := flag.String("stream", "", "Redis stream (for stream mode)")
	group := flag.String("group", "", "Redis consumer group (for stream mode)")
	consumer := flag.String("consumer", "", "Redis consumer name (for stream mode)")
	dataKey := flag.String("dataKey", "data", "Field name for data in stream messages")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: *address})
	defer client.Close()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("\nInterrupted by user")
		cancel()
		os.Exit(0)
	}()

	mode := "channel"
	if *stream != "" {
		mode = "stream"
	}

	if mode == "stream" && *stream != "" {
		fmt.Printf("Listening to Redis stream '%s' on %s\n", *stream, *address)
		lastID := "$"
		useGroup := *group != "" && *consumer != ""
		if useGroup {
			_ = client.XGroupCreateMkStream(ctx, *stream, *group, "0").Err()
			lastID = ">"
		}
		for {
			var res []redis.XStream
			var err error
			if useGroup {
				res, err = client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    *group,
					Consumer: *consumer,
					Streams:  []string{*stream, lastID},
					Count:    1,
					Block:    0,
					NoAck:    false,
				}).Result()
			} else {
				res, err = client.XRead(ctx, &redis.XReadArgs{
					Streams: []string{*stream, lastID},
					Count:   1,
					Block:   0,
				}).Result()
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error reading from stream: %v\n", err)
				time.Sleep(2 * time.Second)
				continue
			}
			for _, xstream := range res {
				for _, xmsg := range xstream.Messages {
					meta := map[string]interface{}{"id": xmsg.ID}
					for k, v := range xmsg.Values {
						meta[k] = v
					}
					var data []byte
					if v, ok := xmsg.Values[*dataKey]; ok {
						switch val := v.(type) {
						case string:
							data = []byte(val)
						case []byte:
							data = val
						}
					}
					printMsgDetails("stream", xmsg.ID, data, meta)
					if useGroup {
						_ = client.XAck(ctx, *stream, *group, xmsg.ID).Err()
					} else {
						lastID = xmsg.ID
					}
				}
			}
		}
	} else {
		fmt.Printf("Listening to Redis channel '%s' on %s\n", *channel, *address)
		pubsub := client.Subscribe(ctx, *channel)
		ch := pubsub.Channel()
		for msg := range ch {
			meta := map[string]interface{}{"channel": msg.Channel}
			printMsgDetails("channel", msg.Channel, []byte(msg.Payload), meta)
		}
	}
}
