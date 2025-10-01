//go:build integration
// +build integration

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	redisTC "github.com/testcontainers/testcontainers-go/modules/redis"
)

const (
	testChannelPubSub = "test-channel-pubsub"
	testStreamName    = "test-stream"
	testConsumerGroup = "test-consumer-group"
	testConsumerName  = "test-consumer"
	testDataKey       = "data"
	testHeaderKey     = "test-header"
	testHeaderValue   = "test-value"
	testMsgTypeKey    = "msg-type"
)

var (
	redisContainer testcontainers.Container
	redisAddress   string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup Redis container
	redisC, err := redisTC.Run(ctx, "redis:7-alpine")
	if err != nil {
		panic(fmt.Sprintf("failed to start Redis container: %v", err))
	}
	redisContainer = redisC

	// Get Redis address
	host, err := redisC.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get Redis host: %v", err))
	}
	port, err := redisC.MappedPort(ctx, "6379/tcp")
	if err != nil {
		panic(fmt.Sprintf("failed to get Redis port: %v", err))
	}
	redisAddress = fmt.Sprintf("%s:%s", host, port.Port())

	// Run tests
	code := m.Run()

	// Cleanup
	if err := redisContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate Redis container: %v\n", err)
	}

	os.Exit(code)
}

func TestRedisPubSubIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source configuration for PubSub
	sourceCfg := &SourceConfig{
		Address: redisAddress,
		Channel: testChannelPubSub,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup target to send test messages
	targetCfg := &TargetConfig{
		Address: redisAddress,
		Channel: testChannelPubSub,
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send test message
	testData := []byte("test pubsub message data")
	testID := []byte("pubsub-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			testHeaderKey:  testHeaderValue,
			testMsgTypeKey: "pubsub",
		},
	})

	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, testChannelPubSub, metadata["channel"])

		// Acknowledge the message
		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for pubsub message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestRedisChannelFromMetadataIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dynamicChannel := "dynamic-test-channel"

	// Setup source for dynamic channel
	sourceCfg := &SourceConfig{
		Address: redisAddress,
		Channel: dynamicChannel,
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup target with channel from metadata
	targetCfg := &TargetConfig{
		Address:                redisAddress,
		Channel:                dynamicChannel, // Fallback channel
		ChannelFromMetadataKey: "target-channel",
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send test message with channel in metadata
	testData := []byte("dynamic channel test message")
	testID := []byte("dynamic-channel-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			"target-channel": dynamicChannel,
			"test-type":      "dynamic-channel",
		},
	})

	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, dynamicChannel, metadata["channel"])

		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for dynamic channel message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestRedisStreamIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup stream source configuration without consumer group
	sourceCfg := &SourceConfig{
		Address:       redisAddress,
		Stream:        testStreamName,
		StreamDataKey: testDataKey,
		LastID:        "0", // Start from beginning for testing
	}

	// Create stream source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait a bit for the stream consumer to be ready
	time.Sleep(100 * time.Millisecond)

	// Setup stream target
	targetCfg := &TargetConfig{
		Address:       redisAddress,
		Stream:        testStreamName,
		StreamDataKey: testDataKey,
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send test message
	testData := []byte("test stream message data")
	testID := []byte("stream-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			testHeaderKey:  testHeaderValue,
			testMsgTypeKey: "stream",
		},
	})

	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Contains(t, metadata, "id") // Stream message should have ID

		// Acknowledge the message
		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for stream message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestRedisStreamWithConsumerGroupIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use a different stream name for consumer group test to avoid conflicts
	consumerGroupStreamName := "test-stream-consumer-group"

	// Setup stream source configuration with consumer group
	sourceCfg := &SourceConfig{
		Address:       redisAddress,
		Stream:        consumerGroupStreamName,
		ConsumerGroup: testConsumerGroup,
		ConsumerName:  testConsumerName,
		StreamDataKey: testDataKey,
	}

	// Create stream source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait a bit for the stream consumer to be ready
	time.Sleep(100 * time.Millisecond)

	// Setup stream target
	targetCfg := &TargetConfig{
		Address:       redisAddress,
		Stream:        consumerGroupStreamName,
		StreamDataKey: testDataKey,
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send test message
	testData := []byte("test consumer group stream message")
	testID := []byte("consumer-group-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			testHeaderKey:    testHeaderValue,
			testMsgTypeKey:   "stream-consumer-group",
			"consumer-group": testConsumerGroup,
		},
	})

	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Contains(t, metadata, "id") // Stream message should have ID

		// Acknowledge the message
		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for consumer group stream message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestRedisStreamDynamicStreamNameIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dynamicStreamName := "dynamic-test-stream"

	// Setup stream source for dynamic stream
	sourceCfg := &SourceConfig{
		Address:       redisAddress,
		Stream:        dynamicStreamName,
		StreamDataKey: testDataKey,
		LastID:        "0", // Start from beginning for testing
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait a bit for the stream consumer to be ready
	time.Sleep(100 * time.Millisecond)

	// Setup target with stream from metadata
	targetCfg := &TargetConfig{
		Address:               redisAddress,
		Stream:                dynamicStreamName, // Fallback stream
		StreamFromMetadataKey: "target-stream",
		StreamDataKey:         testDataKey,
	}

	target, err := NewTarget(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Send test message with stream in metadata
	testData := []byte("dynamic stream test message")
	testID := []byte("dynamic-stream-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			"target-stream": dynamicStreamName,
			"test-type":     "dynamic-stream",
		},
	})

	err = target.Consume(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Contains(t, metadata, "id") // Stream message should have ID

		err = receivedMsg.Ack()
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for dynamic stream message")
	}

	// Close source explicitly before test ends
	source.Close()
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

func (m *TestMessage) GetMetadata() (message.MessageMetadata, error) {
	meta := make(message.MessageMetadata)
	for k, v := range m.metadata {
		meta[k] = v
	}
	return meta, nil
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
