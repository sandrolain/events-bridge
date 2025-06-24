package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/fatih/color"
)

func printMsgDetails(topic string, payload []byte) {
	black := color.New(color.FgBlack).Add(color.ResetUnderline).PrintfFunc()
	blue := color.New(color.FgHiBlue).Add(color.Underline).PrintfFunc()
	white := color.New(color.FgWhite).Add(color.ResetUnderline).PrintfFunc()

	black("\n----------------------------------------\n")
	black(time.Now().Format(time.RFC3339) + "\n")
	blue("Topic:\n")
	white("  %s\n", topic)
	blue("Payload:\n")
	if len(payload) > 0 {
		white("%s\n\n", string(payload))
	} else {
		white("<empty>\n\n")
	}
}

func main() {
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker address")
	topic := flag.String("topic", "test", "MQTT topic to subscribe to")
	clientID := flag.String("clientid", "mqtttarget-test", "MQTT client ID")
	flag.Parse()

	fmt.Printf("Starting mqtttarget on %s, topic '%s'\n", *broker, *topic)

	opts := mqtt.NewClientOptions().AddBroker(*broker).SetClientID(*clientID)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to MQTT broker: %v\n", token.Error())
		os.Exit(1)
	}
	defer client.Disconnect(250)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigc
		fmt.Println("\nInterrupted by user")
		os.Exit(0)
	}()

	if token := client.Subscribe(*topic, 0, func(_ mqtt.Client, msg mqtt.Message) {
		printMsgDetails(msg.Topic(), msg.Payload())
	}); token.Wait() && token.Error() != nil {
		fmt.Fprintf(os.Stderr, "Error subscribing to topic: %v\n", token.Error())
		os.Exit(1)
	}

	fmt.Println("Listening for MQTT messages. Press Ctrl+C to exit.")
	select {}
}
