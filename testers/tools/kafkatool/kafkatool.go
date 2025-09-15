package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"

	toolutil "github.com/sandrolain/events-bridge/testers/tools/toolutil"
)

func main() {
	root := &cobra.Command{
		Use:   "kafkacli",
		Short: "Kafka client tester",
		Long:  "A simple Kafka CLI with send and serve commands.",
	}

	// SEND command
	var (
		sendBrokers     string
		sendTopic       string
		sendPayload     string
		sendInterval    string
		sendTestPayload string
	)
	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Produce periodic Kafka messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}

			w := kafka.NewWriter(kafka.WriterConfig{
				Brokers: strings.Split(sendBrokers, ","),
				Topic:   sendTopic,
			})
			defer func() { _ = w.Close() }()

			ticker := time.NewTicker(dur)
			defer ticker.Stop()
			fmt.Printf("Producing to Kafka %s, topic: %s\n", sendBrokers, sendTopic)

			for range ticker.C {
				body, _, err := toolutil.BuildPayload(sendTestPayload, sendPayload, toolutil.CTText)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err = w.WriteMessages(ctx, kafka.Message{Value: body})
				cancel()
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error sending message: %v\n", err)
				} else {
					fmt.Println("Message sent to Kafka")
				}
			}
			return nil
		},
	}
	sendCmd.Flags().StringVar(&sendBrokers, "brokers", "localhost:9092", "Kafka brokers (comma-separated)")
	sendCmd.Flags().StringVar(&sendTopic, "topic", "test", "Kafka topic")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "Hello, Kafka!", new(string), "", &sendTestPayload)
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")

	// SERVE command
	var (
		subBrokers string
		subTopic   string
		subGroup   string
	)
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Consume messages and print them",
		RunE: func(cmd *cobra.Command, args []string) error {
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:  strings.Split(subBrokers, ","),
				GroupID:  subGroup,
				Topic:    subTopic,
				MinBytes: 1,
				MaxBytes: 10e6,
			})
			defer func() { _ = r.Close() }()

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
			go func() { <-sigc; fmt.Println("\nInterrupted by user"); os.Exit(0) }()

			fmt.Printf("Consuming from Kafka %s, topic: %s, group: %s\n", subBrokers, subTopic, subGroup)
			for {
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error reading message: %v\n", err)
					break
				}
				// Build sections with metadata
				var headerItems []toolutil.KV
				for _, h := range m.Headers {
					headerItems = append(headerItems, toolutil.KV{Key: h.Key, Value: string(h.Value)})
				}
				sections := []toolutil.MessageSection{
					{Title: "Topic", Items: []toolutil.KV{{Key: "Name", Value: m.Topic}}},
					{Title: "Meta", Items: []toolutil.KV{
						{Key: "Partition", Value: strconv.Itoa(m.Partition)},
						{Key: "Offset", Value: strconv.FormatInt(m.Offset, 10)},
						{Key: "Time", Value: m.Time.Format(time.RFC3339)},
					}},
					{Title: "Key", Items: []toolutil.KV{{Key: "Value", Value: string(m.Key)}}},
					{Title: "Headers", Items: headerItems},
				}
				ct := toolutil.GuessMIME(m.Value)
				toolutil.PrintColoredMessage("Kafka", sections, m.Value, ct)
			}
			return nil
		},
	}
	serveCmd.Flags().StringVar(&subBrokers, "brokers", "localhost:9092", "Kafka brokers (comma-separated)")
	serveCmd.Flags().StringVar(&subTopic, "topic", "test", "Kafka topic")
	serveCmd.Flags().StringVar(&subGroup, "group", "", "Kafka consumer group")

	root.AddCommand(sendCmd, serveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
