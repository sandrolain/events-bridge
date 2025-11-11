//go:build integration

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
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	testIntegrationSubject = "test.integration.subject"
	testQueueGroup         = "test-queue-group"
	testHeaderKey          = "test-header"
	testHeaderValue        = "test-value"
	testMsgTypeKey         = "msg-type"
)

var (
	natsContainer testcontainers.Container
	natsAddress   string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup NATS container
	req := testcontainers.ContainerRequest{
		Image:        "nats:2.10-alpine",
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForLog("Server is ready"),
	}

	natsC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to start NATS container: %v", err))
	}
	natsContainer = natsC

	// Get NATS address
	host, err := natsC.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get NATS host: %v", err))
	}
	port, err := natsC.MappedPort(ctx, "4222/tcp")
	if err != nil {
		panic(fmt.Sprintf("failed to get NATS port: %v", err))
	}
	natsAddress = fmt.Sprintf("nats://%s:%s", host, port.Port())

	// Run tests
	code := m.Run()

	// Cleanup
	if err := natsContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate NATS container: %v\n", err)
	}

	os.Exit(code)
}

func TestNATSPubSubIntegration(t *testing.T) {
	// Setup source configuration for standard pub/sub
	sourceCfg := &SourceConfig{
		Address: natsAddress,
		Subject: testIntegrationSubject,
		Mode:    "subscribe",
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup runner to send test messages
	runnerCfg := &RunnerConfig{
		Address: natsAddress,
		Subject: testIntegrationSubject,
		Mode:    "publish",
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Wait a bit for subscription to be ready
	time.Sleep(200 * time.Millisecond)

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

	err = runner.Process(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, testIntegrationSubject, metadata["subject"])

		// Acknowledge the message
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for pubsub message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestNATSSubjectFromMetadataIntegration(t *testing.T) {
	dynamicSubject := "dynamic.test.subject"

	// Setup source for dynamic subject
	sourceCfg := &SourceConfig{
		Address: natsAddress,
		Subject: dynamicSubject,
		Mode:    "subscribe",
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup runner with subject from metadata
	runnerCfg := &RunnerConfig{
		Address:                natsAddress,
		Subject:                "fallback.subject",
		SubjectFromMetadataKey: "target-subject",
		Mode:                   "publish",
		Timeout:                5 * time.Second,
	}

	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Wait a bit for subscription to be ready
	time.Sleep(200 * time.Millisecond)

	// Send test message with subject in metadata
	testData := []byte("dynamic subject test message")
	testID := []byte("dynamic-subject-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			"target-subject": dynamicSubject,
			"test-type":      "dynamic-subject",
		},
	})

	err = runner.Process(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, dynamicSubject, metadata["subject"])

		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for dynamic subject message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestNATSQueueGroupIntegration(t *testing.T) {
	queueSubject := "queue.test.subject"

	// Setup two sources with same queue group
	sourceCfg1 := &SourceConfig{
		Address:    natsAddress,
		Subject:    queueSubject,
		QueueGroup: testQueueGroup,
		Mode:       "subscribe",
	}

	source1, err := NewSource(sourceCfg1)
	require.NoError(t, err)
	defer source1.Close()

	msgChan1, err := source1.Produce(10)
	require.NoError(t, err)

	sourceCfg2 := &SourceConfig{
		Address:    natsAddress,
		Subject:    queueSubject,
		QueueGroup: testQueueGroup,
		Mode:       "subscribe",
	}

	source2, err := NewSource(sourceCfg2)
	require.NoError(t, err)
	defer source2.Close()

	msgChan2, err := source2.Produce(10)
	require.NoError(t, err)

	// Setup runner
	runnerCfg := &RunnerConfig{
		Address: natsAddress,
		Subject: queueSubject,
		Mode:    "publish",
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Wait a bit for subscriptions to be ready
	time.Sleep(200 * time.Millisecond)

	// Send test message
	testData := []byte("queue group test message")
	testID := []byte("queue-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			testMsgTypeKey: "queue-group",
		},
	})

	err = runner.Process(testMsg)
	require.NoError(t, err)

	// Only one of the queue group members should receive the message
	received := false
	select {
	case receivedMsg := <-msgChan1:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)
		received = true

	case receivedMsg := <-msgChan2:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)
		received = true

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for queue group message")
	}

	assert.True(t, received, "message should be received by one queue member")

	// Ensure the other source doesn't receive the message
	select {
	case <-msgChan1:
		t.Fatal("message received by both queue members")
	case <-msgChan2:
		t.Fatal("message received by both queue members")
	case <-time.After(1 * time.Second):
		// Expected - only one queue member should receive
	}
}

func TestNATSWildcardSubjectIntegration(t *testing.T) {
	// Setup source with wildcard subject
	sourceCfg := &SourceConfig{
		Address: natsAddress,
		Subject: "wildcard.*.subject",
		Mode:    "subscribe",
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup runner
	runnerCfg := &RunnerConfig{
		Address: natsAddress,
		Subject: "wildcard.test.subject",
		Mode:    "publish",
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Wait a bit for subscription to be ready
	time.Sleep(200 * time.Millisecond)

	// Send test message
	testData := []byte("wildcard test message")
	testID := []byte("wildcard-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			testMsgTypeKey: "wildcard",
		},
	})

	err = runner.Process(testMsg)
	require.NoError(t, err)

	// Wait for message to be received
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, "wildcard.test.subject", metadata["subject"])

		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for wildcard message")
	}

	// Close source explicitly before test ends
	source.Close()
}

func TestNATSMultipleMessagesIntegration(t *testing.T) {
	multiSubject := "multi.messages.subject"

	// Setup source
	sourceCfg := &SourceConfig{
		Address: natsAddress,
		Subject: multiSubject,
		Mode:    "subscribe",
	}

	source, err := NewSource(sourceCfg)
	require.NoError(t, err)

	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Setup runner
	runnerCfg := &RunnerConfig{
		Address: natsAddress,
		Subject: multiSubject,
		Mode:    "publish",
		Timeout: 5 * time.Second,
	}

	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)
	defer runner.Close()

	// Wait a bit for subscription to be ready
	time.Sleep(200 * time.Millisecond)

	// Send multiple test messages
	messageCount := 5
	for i := 0; i < messageCount; i++ {
		testData := []byte(fmt.Sprintf("message %d", i))
		testID := []byte(fmt.Sprintf("multi-test-id-%d", i))
		testMsg := message.NewRunnerMessage(&TestMessage{
			id:   testID,
			data: testData,
			metadata: map[string]string{
				testMsgTypeKey: "multi",
				"sequence":     fmt.Sprintf("%d", i),
			},
		})

		err = runner.Process(testMsg)
		require.NoError(t, err)
	}

	// Receive all messages
	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < messageCount {
		select {
		case receivedMsg := <-msgChan:
			data, err := receivedMsg.GetSourceData()
			require.NoError(t, err)
			assert.Contains(t, string(data), "message")

			err = receivedMsg.Ack(nil)
			assert.NoError(t, err)

			receivedCount++

		case <-timeout:
			t.Fatalf("timeout waiting for messages, received %d of %d", receivedCount, messageCount)
		}
	}

	assert.Equal(t, messageCount, receivedCount, "should receive all messages")

	// Close source explicitly before test ends
	source.Close()
}

func TestNATSReconnectionIntegration(t *testing.T) {
	reconnectSubject := "reconnect.test.subject"

	// Setup runner with reconnection settings
	runnerCfg := &RunnerConfig{
		Address:       natsAddress,
		Subject:       reconnectSubject,
		Mode:          "publish",
		Timeout:       5 * time.Second,
		MaxReconnects: 10,
		ReconnectWait: 1 * time.Second,
	}

	runner, err := NewRunner(runnerCfg)
	require.NoError(t, err)

	// Verify runner is created successfully with reconnection config
	natsRunner, ok := runner.(*NATSRunner)
	require.True(t, ok, "expected *NATSRunner")
	assert.Equal(t, 10, natsRunner.cfg.MaxReconnects)
	assert.Equal(t, 1*time.Second, natsRunner.cfg.ReconnectWait)

	// Send a test message to verify connection works
	testData := []byte("reconnection test message")
	testID := []byte("reconnect-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:   testID,
		data: testData,
		metadata: map[string]string{
			testMsgTypeKey: "reconnect",
		},
	})

	err = runner.Process(testMsg)
	require.NoError(t, err, "message should be sent successfully")

	runner.Close()
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
	meta := make(map[string]string)
	for k, v := range m.metadata {
		meta[k] = v
	}
	return meta, nil
}

func (m *TestMessage) GetData() ([]byte, error) {
	return m.data, nil
}

func (m *TestMessage) Ack(*message.ReplyData) error {
	return nil
}

func (m *TestMessage) Nak() error {
	return nil
}
