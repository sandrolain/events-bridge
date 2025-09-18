package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	toolutil "github.com/sandrolain/events-bridge/testers/tools/toolutil"
	"github.com/spf13/cobra"
)

func main() {
	const tcpPrefix = "tcp://"
	const sslPrefix = "ssl://"
	const wsPrefix = "ws://"
	root := &cobra.Command{
		Use:   "mqttcli",
		Short: "MQTT client/server tester",
		Long:  "A simple MQTT client/server CLI with send and serve commands.",
	}

	// SEND command
	var (
		sendBroker   string
		sendTopic    string
		sendPayload  string
		sendInterval string
		sendQoS      int
		sendRetain   bool
		sendClientID string
	)
	sendCmd := &cobra.Command{
		Use:   "send",
		Short: "Publish periodic MQTT messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !strings.HasPrefix(sendBroker, tcpPrefix) && !strings.HasPrefix(sendBroker, sslPrefix) && !strings.HasPrefix(sendBroker, wsPrefix) {
				sendBroker = tcpPrefix + sendBroker
			}
			opts := mqtt.NewClientOptions().AddBroker(sendBroker)
			if sendClientID == "" {
				sendClientID = fmt.Sprintf("mqttcli-pub-%d", time.Now().UnixNano())
			}
			opts.SetClientID(sendClientID).SetAutoReconnect(true)
			client := mqtt.NewClient(opts)
			if token := client.Connect(); token.Wait() && token.Error() != nil {
				return fmt.Errorf("MQTT connection error: %w", token.Error())
			}
			defer client.Disconnect(250)

			dur, err := time.ParseDuration(sendInterval)
			if err != nil {
				return fmt.Errorf("invalid interval: %w", err)
			}
			ticker := time.NewTicker(dur)
			defer ticker.Stop()

			fmt.Printf("Connected to %s, topic: %s\n", sendBroker, sendTopic)

			publish := func() {
				body, _, err := toolutil.BuildPayload(sendPayload, toolutil.CTText)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					return
				}
				token := client.Publish(sendTopic, byte(sendQoS), sendRetain, body)
				token.Wait()
				if token.Error() != nil {
					fmt.Fprintf(os.Stderr, "Publish error: %v\n", token.Error())
				} else {
					fmt.Printf("Payload sent to %s (%d bytes)\n", sendTopic, len(body))
				}
			}

			for range ticker.C {
				publish()
			}
			return nil
		},
	}
	sendCmd.Flags().StringVar(&sendBroker, "broker", "tcp://localhost:1883", "MQTT broker URL (tcp://host:port)")
	sendCmd.Flags().StringVar(&sendTopic, "topic", "test/topic", "MQTT topic to publish to")
	sendCmd.Flags().IntVar(&sendQoS, "qos", 0, "MQTT QoS level (0,1,2)")
	sendCmd.Flags().BoolVar(&sendRetain, "retain", false, "Retain messages")
	sendCmd.Flags().StringVar(&sendClientID, "clientid", "", "Client ID (auto if empty)")
	toolutil.AddPayloadFlags(sendCmd, &sendPayload, "{nowtime}", new(string), "")
	toolutil.AddIntervalFlag(sendCmd, &sendInterval, "5s")

	// SERVE command
	var (
		subBroker   string
		subTopic    string
		subClientID string
		subQoS      int
	)
	serveCmd := &cobra.Command{
		Use:   "serve",
		Short: "Subscribe to a topic and log messages",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !strings.HasPrefix(subBroker, tcpPrefix) && !strings.HasPrefix(subBroker, sslPrefix) && !strings.HasPrefix(subBroker, wsPrefix) {
				subBroker = tcpPrefix + subBroker
			}
			if subClientID == "" {
				subClientID = fmt.Sprintf("mqttcli-sub-%d", time.Now().UnixNano())
			}

			opts := mqtt.NewClientOptions().AddBroker(subBroker).SetClientID(subClientID)
			client := mqtt.NewClient(opts)
			if token := client.Connect(); token.Wait() && token.Error() != nil {
				return fmt.Errorf("Error connecting to MQTT broker: %w", token.Error())
			}
			defer client.Disconnect(250)

			fmt.Printf("Listening on %s, topic '%s'\n", subBroker, subTopic)

			if token := client.Subscribe(subTopic, byte(subQoS), func(_ mqtt.Client, msg mqtt.Message) {
				ct := toolutil.GuessMIME(msg.Payload())
				sections := []toolutil.MessageSection{
					{Title: "Topic", Items: []toolutil.KV{{Key: "Name", Value: msg.Topic()}}},
				}
				toolutil.PrintColoredMessage("MQTT", sections, msg.Payload(), ct)
			}); token.Wait() && token.Error() != nil {
				return fmt.Errorf("Error subscribing to topic: %w", token.Error())
			}

			sigc := make(chan os.Signal, 1)
			signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
			<-sigc
			fmt.Println("\nInterrupted by user")
			return nil
		},
	}
	serveCmd.Flags().StringVar(&subBroker, "broker", "tcp://localhost:1883", "MQTT broker URL (tcp://host:port)")
	serveCmd.Flags().StringVar(&subTopic, "topic", "test/topic", "MQTT topic to subscribe to")
	serveCmd.Flags().StringVar(&subClientID, "clientid", "", "Client ID (auto if empty)")
	serveCmd.Flags().IntVar(&subQoS, "qos", 0, "MQTT QoS level (0,1,2)")

	root.AddCommand(sendCmd, serveCmd)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
