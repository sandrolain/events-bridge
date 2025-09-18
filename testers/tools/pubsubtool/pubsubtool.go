package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
	toolutil "github.com/sandrolain/events-bridge/testers/tools/toolutil"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "pubsubcli",
		Short: "Google Pub/Sub client tester",
		Long:  "A simple Google Cloud Pub/Sub CLI with send and serve commands.",
	}

	// SEND
	var (
		sendProject  string
		sendTopic    string
		sendPayload  string
		sendInterval string
	)
	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Publish periodic Pub/Sub messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client, err := pubsub.NewClient(ctx, sendProject)
			if err != nil {
				return fmt.Errorf("Pub/Sub client error: %w", err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to close Pub/Sub client: %v\n", err)
				}
			}()

			publisher := client.Publisher(sendTopic)
			defer publisher.Stop()

			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			ticker := time.NewTicker(dur)
			defer ticker.Stop()

			fmt.Printf("Publishing to project %s, topic %s every %s\n", sendProject, sendTopic, dur)
			for range ticker.C {
				body, _, err := toolutil.BuildPayload(sendPayload, toolutil.CTText)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				result := publisher.Publish(ctx, &pubsub.Message{Data: body})
				id, err := result.Get(ctx)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error sending message: %v\n", err)
				} else {
					fmt.Printf("Message successfully sent! ID: %s\n", id)
				}
			}
			return nil
		},
	}
	sendCmd.Flags().StringVar(&sendProject, "project", "test-project", "Google Cloud Project ID")
	sendCmd.Flags().StringVar(&sendTopic, "topic", "test-topic", "Pub/Sub topic ID")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "Hello, PubSub!", new(string), "")
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")

	// SERVE
	var (
		subProject string
		subSub     string
	)
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Subscribe and log Pub/Sub messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			client, err := pubsub.NewClient(ctx, subProject)
			if err != nil {
				return fmt.Errorf("Pub/Sub client error: %w", err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to close Pub/Sub client: %v\n", err)
				}
			}()

			sub := client.Subscriber(subSub)

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
			go func() { <-sigc; fmt.Println("\nInterrupted by user"); cancel() }()

			fmt.Printf("Listening to Pub/Sub project %s, subscription: %s\n", subProject, subSub)
			for {
				err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
					// Build sections: Topic/Subscription unknown in message directly; include attributes and meta
					var attrItems []toolutil.KV
					for k, v := range m.Attributes {
						attrItems = append(attrItems, toolutil.KV{Key: k, Value: v})
					}
					sections := []toolutil.MessageSection{
						{Title: "Subscription", Items: []toolutil.KV{{Key: "Name", Value: subSub}}},
						{Title: "Meta", Items: []toolutil.KV{{Key: "PublishTime", Value: m.PublishTime.Format(time.RFC3339)}}},
						{Title: "Attributes", Items: attrItems},
					}
					ct := toolutil.GuessMIME(m.Data)
					toolutil.PrintColoredMessage("Pub/Sub", sections, m.Data, ct)
					m.Ack()
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "Receive error: %v\n", err)
					time.Sleep(2 * time.Second)
				} else {
					break
				}
			}
			return nil
		},
	}
	serveCmd.Flags().StringVar(&subProject, "project", "test-project", "Google Cloud Project ID")
	serveCmd.Flags().StringVar(&subSub, "subscription", "test-subscription", "Pub/Sub subscription ID")

	root.AddCommand(sendCmd, serveCmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
