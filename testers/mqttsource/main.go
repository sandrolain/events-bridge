package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	address := flag.String("address", "localhost:1883", "MQTT broker address:port")
	topic := flag.String("topic", "test/topic", "MQTT topic")
	payload := flag.String("payload", "hello", "Payload to send")
	interval := flag.Int("interval", 5, "Send interval in seconds")
	flag.Parse()

	broker := "tcp://" + *address
	opts := mqtt.NewClientOptions().AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("mqtt-tester-%d", time.Now().UnixNano()))
	opts.SetAutoReconnect(true)
	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		fmt.Fprintf(os.Stderr, "MQTT connection error: %v\n", token.Error())
		os.Exit(1)
	}
	fmt.Printf("Connected to %s, topic: %s\n", broker, *topic)

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		token := client.Publish(*topic, 0, false, *payload)
		token.Wait()
		if token.Error() != nil {
			fmt.Fprintf(os.Stderr, "Publish error: %v\n", token.Error())
		} else {
			fmt.Printf("Payload sent to %s: %s\n", *topic, *payload)
		}
	}
}
