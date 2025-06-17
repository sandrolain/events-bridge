package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/nats-io/nats.go"
)

func printMsgDetails(msg *nats.Msg) {
	black := color.New(color.FgBlack).Add(color.ResetUnderline).PrintfFunc()
	blue := color.New(color.FgHiBlue).Add(color.Underline).PrintfFunc()
	white := color.New(color.FgWhite).Add(color.ResetUnderline).PrintfFunc()

	black("\n----------------------------------------\n")
	black(time.Now().Format(time.RFC3339) + "\n")
	blue("Subject:\n")
	white("  %s\n", msg.Subject)
	if msg.Reply != "" {
		blue("Reply to:\n")
		white("  %s\n", msg.Reply)
	}
	if len(msg.Header) > 0 {
		blue("Headers:\n")
		for k, v := range msg.Header {
			white("  %s: %s\n", k, strings.Join(v, ", "))
		}
	}
	blue("Payload:\n")
	if len(msg.Data) > 0 {
		white("%s\n\n", string(msg.Data))
	} else {
		white("<empty>\n\n")
	}
}

func main() {
	address := flag.String("address", nats.DefaultURL, "NATS server URL")
	subject := flag.String("subject", "test", "NATS subject to listen on")
	flag.Parse()

	log.Printf("Starting natstarget on %s, subject '%s'", *address, *subject)

	nc, err := nats.Connect(*address)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	sub, err := nc.Subscribe(*subject, func(msg *nats.Msg) {
		printMsgDetails(msg)
		if msg.Reply != "" {
			err := nc.Publish(msg.Reply, []byte("OK"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error sending reply: %v\n", err)
			}
		}
	})
	if err != nil {
		log.Fatalf("Error subscribing to subject: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("Listening for messages. Press Ctrl+C to exit.")
	select {}
}
