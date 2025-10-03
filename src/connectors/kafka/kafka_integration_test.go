//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	testTopicSource    = "test-topic-source"
	testTopicTarget    = "test-topic-target"
	testTopicRoundtrip = "test-topic-roundtrip"
)

var (
	kafkaContainer testcontainers.Container
	kafkaBrokers   []string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup Kafka container
	kafkaC, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		panic(fmt.Sprintf("failed to start Kafka container: %v", err))
	}
	kafkaContainer = kafkaC

	// Get broker address
	brokers, err := kafkaC.Brokers(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get Kafka brokers: %v", err))
	}
	kafkaBrokers = brokers

	// Run tests
	code := m.Run()

	// Cleanup
	if err := kafkaContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate Kafka container: %v\n", err)
	}

	os.Exit(code)
}

func TestKafkaSourceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source configuration
	sourceCfg := &SourceConfig{
		Brokers:           kafkaBrokers,
		GroupID:           "test-group",
		Topic:             testTopicSource,
		Partitions:        1,
		ReplicationFactor: 1,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start producing messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup target to send test messages
	targetCfg := &TargetConfig{
		Brokers:           kafkaBrokers,
		Topic:             testTopicSource,
		Partitions:        1,
		ReplicationFactor: 1,
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send test message
	testData := []byte("test message data")
	testID := []byte("test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			"test-header": "test-value",
		},
	})

	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		assert.Equal(t, testID, receivedMsg.GetID())

		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, testTopicSource, metadata["topic"])
		assert.Equal(t, "0", metadata["partition"]) // Single partition
		assert.NotEmpty(t, metadata["offset"])

		// Acknowledge the message
		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestKafkaTargetIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup target configuration
	targetCfg := &TargetConfig{
		Brokers:           kafkaBrokers,
		Topic:             testTopicTarget,
		Partitions:        1,
		ReplicationFactor: 1,
	}

	// Create target
	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Create test message
	testData := []byte("integration test message")
	testID := []byte("integration-test-id")
	testMetadata := map[string]string{
		"test-header":    "test-value",
		"integration":    "true",
		"message-number": "42",
	}

	testMsg := message.NewRunnerMessage(&TestMessage{
		id:       testID,
		data:     testData,
		metadata: testMetadata,
	})

	// Send message through target
	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Setup source to verify message was sent
	sourceCfg := &SourceConfig{
		Brokers:           kafkaBrokers,
		GroupID:           "test-verify-group",
		Topic:             testTopicTarget,
		Partitions:        1,
		ReplicationFactor: 1,
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Verify message was received
	select {
	case receivedMsg := <-msgChan:
		assert.Equal(t, testID, receivedMsg.GetID())

		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		// Acknowledge the message
		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for verification message")
	}
}

func TestKafkaRoundTripIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	topicName := testTopicRoundtrip

	// Setup source
	sourceCfg := &SourceConfig{
		Brokers:           kafkaBrokers,
		GroupID:           "roundtrip-group",
		Topic:             topicName,
		Partitions:        1,
		ReplicationFactor: 1,
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup target
	targetCfg := &TargetConfig{
		Brokers:           kafkaBrokers,
		Topic:             topicName,
		Partitions:        1,
		ReplicationFactor: 1,
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send multiple test messages
	numMessages := 5
	sentMessages := make(map[string][]byte)

	for i := 0; i < numMessages; i++ {
		testData := []byte(fmt.Sprintf("roundtrip message %d", i))
		testID := []byte(fmt.Sprintf("roundtrip-id-%d", i))

		testMsg := message.NewRunnerMessage(&TestMessage{
			id:   testID,
			data: testData,
			metadata: map[string]string{
				"message-index": fmt.Sprintf("%d", i),
				"test-type":     "roundtrip",
			},
		})

		err = target.Consume(testMsg)
		require.NoError(t, err)

		sentMessages[string(testID)] = testData
	}

	// Receive and verify all messages
	receivedCount := 0
	for receivedCount < numMessages {
		select {
		case receivedMsg := <-msgChan:
			msgID := string(receivedMsg.GetID())
			expectedData, exists := sentMessages[msgID]
			require.True(t, exists, "received unexpected message ID: %s", msgID)

			data, err := receivedMsg.GetSourceData()
			require.NoError(t, err)
			assert.Equal(t, expectedData, data)

			metadata, err := receivedMsg.GetSourceMetadata()
			require.NoError(t, err)
			assert.Equal(t, topicName, metadata["topic"])

			err = receivedMsg.Ack()
			assert.NoError(t, err)

			receivedCount++

		case <-ctx.Done():
			t.Fatalf("timeout waiting for messages, received %d out of %d", receivedCount, numMessages)
		}
	}

	assert.Equal(t, numMessages, receivedCount)
}

// TestMessage is a test implementation of message.SourceMessage
type TestMessage struct {
	id       []byte
	data     []byte
	metadata map[string]string
}

func (m *TestMessage) GetID() []byte {
	return m.id
}

func (m *TestMessage) GetMetadata() (map[string]string, error) {
	return common.CopyMap(m.metadata, nil), nil
}

func (m *TestMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *TestMessage) Ack() error {
	return nil
}

func (m *TestMessage) Nak() error {
	return nil
}

func (m *TestMessage) Reply(data *message.ReplyData) error {
	return nil
}
