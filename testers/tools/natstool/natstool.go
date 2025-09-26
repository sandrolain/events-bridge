package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	toolutil "github.com/sandrolain/events-bridge/testers/toolutil"
	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "natscli",
		Short: "NATS client tester",
		Long:  "A simple NATS CLI with send and serve commands (supports JetStream).",
	}

	// SEND
	var (
		sendAddr     string
		sendSubject  string
		sendPayload  string
		sendInterval string
		sendStream   string
	)
	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Publish periodic NATS messages (JetStream if --stream set)",
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, err := nats.Connect(sendAddr)
			if err != nil {
				return fmt.Errorf("NATS connection error: %w", err)
			}
			defer nc.Close()

			var js nats.JetStreamContext
			if sendStream != "" {
				if js, err = nc.JetStream(); err != nil {
					return fmt.Errorf("JetStream context error: %w", err)
				}
				fmt.Printf("Connected to %s, subject: %s (JetStream), stream: %s\n", sendAddr, sendSubject, sendStream)
			} else {
				fmt.Printf("Connected to %s, subject: %s\n", sendAddr, sendSubject)
			}

			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			ticker := time.NewTicker(dur)
			defer ticker.Stop()

			for range ticker.C {
				body, _, err := toolutil.BuildPayload(sendPayload, toolutil.CTText)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				if sendStream != "" {
					ack, err := js.Publish(sendSubject, body)
					if err != nil {
						fmt.Fprintf(os.Stderr, "JetStream publish error: %v\n", err)
					} else {
						fmt.Printf("Sent (JetStream) seq: %d\n", ack.Sequence)
					}
				} else {
					if err := nc.Publish(sendSubject, body); err != nil {
						fmt.Fprintf(os.Stderr, "Publish error: %v\n", err)
					} else {
						fmt.Printf("Payload sent (%d bytes)\n", len(body))
					}
				}
			}
			return nil
		},
	}
	sendCmd.Flags().StringVar(&sendAddr, "address", nats.DefaultURL, "NATS server URL")
	sendCmd.Flags().StringVar(&sendSubject, "subject", "test.subject", "NATS subject")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "{nowtime}", new(string), "")
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")
	sendCmd.Flags().StringVar(&sendStream, "stream", "", "JetStream stream name (if set, uses JetStream)")

	// SERVE
	var (
		subAddr    string
		subSubject string
		subStream  string
		subDurable string
	)
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Subscribe to a subject and log messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			nc, err := nats.Connect(subAddr)
			if err != nil {
				return fmt.Errorf("Error connecting to NATS: %w", err)
			}
			defer nc.Close()

			// Shared handler
			handler := func(msg *nats.Msg) {
				sections := []toolutil.MessageSection{{Title: "Subject", Items: []toolutil.KV{{Key: "Name", Value: msg.Subject}}}}
				if msg.Reply != "" {
					sections = append(sections, toolutil.MessageSection{Title: "Reply", Items: []toolutil.KV{{Key: "To", Value: msg.Reply}}})
				}
				if len(msg.Header) > 0 {
					var headerItems []toolutil.KV
					for k, v := range msg.Header {
						headerItems = append(headerItems, toolutil.KV{Key: k, Value: fmt.Sprintf("%v", v)})
					}
					sections = append(sections, toolutil.MessageSection{Title: "Headers", Items: headerItems})
				}
				ct := toolutil.GuessMIME(msg.Data)
				toolutil.PrintColoredMessage("NATS", sections, msg.Data, ct)
				if msg.Reply != "" {
					if err := nc.Publish(msg.Reply, []byte("OK")); err != nil {
						fmt.Fprintf(os.Stderr, "Error sending reply: %v\n", err)
					}
				}
			}

			var sub *nats.Subscription
			if subStream != "" {
				js, err := nc.JetStream()
				if err != nil {
					return fmt.Errorf("JetStream context error: %w", err)
				}
				fmt.Printf("Listening (JetStream) on %s, subject '%s', stream '%s'\n", subAddr, subSubject, subStream)
				opts := []nats.SubOpt{nats.BindStream(subStream), nats.DeliverNew()}
				if subDurable != "" {
					opts = append(opts, nats.Durable(subDurable))
				}
				sub, err = js.Subscribe(subSubject, handler, opts...)
				if err != nil {
					return fmt.Errorf("Error subscribing (JetStream): %w", err)
				}
			} else {
				fmt.Printf("Listening on %s, subject '%s'\n", subAddr, subSubject)
				sub, err = nc.Subscribe(subSubject, handler)
				if err != nil {
					return fmt.Errorf("Error subscribing to subject: %w", err)
				}
			}
			defer func() { _ = sub.Unsubscribe() }()

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
			<-sigc
			fmt.Println("\nInterrupted by user")
			return nil
		},
	}
	serveCmd.Flags().StringVar(&subAddr, "address", nats.DefaultURL, "NATS server URL")
	serveCmd.Flags().StringVar(&subSubject, "subject", "test", "NATS subject to listen on")
	serveCmd.Flags().StringVar(&subStream, "stream", "", "JetStream stream name (if set, uses JetStream consumer)")
	serveCmd.Flags().StringVar(&subDurable, "durable", "", "JetStream durable consumer name (optional)")

	root.AddCommand(sendCmd, serveCmd)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
