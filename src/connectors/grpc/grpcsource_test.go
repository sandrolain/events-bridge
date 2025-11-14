package main

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/connectors/grpc/proto"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestNewSourceConfig(t *testing.T) {
	cfg := NewSourceConfig()
	require.NotNil(t, cfg)
	_, ok := cfg.(*SourceConfig)
	assert.True(t, ok)
}

func TestNewSource(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &SourceConfig{
			Address: "localhost:50051",
		}
		source, err := NewSource(cfg)
		require.NoError(t, err)
		require.NotNil(t, source)
		defer source.Close()
	})

	t.Run("invalid config type", func(t *testing.T) {
		source, err := NewSource("invalid")
		assert.Error(t, err)
		assert.Nil(t, source)
		assert.Contains(t, err.Error(), "invalid config type")
	})
}

func TestGRPCSource_SendEvent(t *testing.T) {
	t.Run("successful message", func(t *testing.T) {
		cfg := &SourceConfig{
			Address:               "localhost:50051",
			MaxReceiveMessageSize: 10 * 1024 * 1024,
		}
		source, err := NewSource(cfg)
		require.NoError(t, err)
		defer source.Close()

		msgChan, err := source.Produce(10)
		require.NoError(t, err)
		require.NotNil(t, msgChan)

		time.Sleep(500 * time.Millisecond)

		conn, err := grpc.NewClient("localhost:50051",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(10*1024*1024),
				grpc.MaxCallSendMsgSize(10*1024*1024),
			),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := proto.NewEventBridgeServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		uuid := make([]byte, 16)
		_, _ = rand.Read(uuid)

		msg := &proto.EventMessage{
			Uuid: uuid,
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			Data: []byte("test data"),
		}

		go func() {
			select {
			case runnerMsg := <-msgChan:
				assert.NotNil(t, runnerMsg)
				metadata, err := runnerMsg.GetMetadata()
				assert.NoError(t, err)
				assert.Equal(t, "value1", metadata["key1"])

				data, err := runnerMsg.GetData()
				assert.NoError(t, err)
				assert.Equal(t, []byte("test data"), data)

				err = runnerMsg.Ack(nil)
				assert.NoError(t, err)
			case <-ctx.Done():
				t.Error("timeout waiting for message")
			}
		}()

		resp, err := client.SendEvent(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Success)
	})

	t.Run("message with reply", func(t *testing.T) {
		cfg := &SourceConfig{
			Address:               "localhost:50054",
			MaxReceiveMessageSize: 10 * 1024 * 1024,
		}
		source, err := NewSource(cfg)
		require.NoError(t, err)
		defer source.Close()

		msgChan, err := source.Produce(10)
		require.NoError(t, err)
		require.NotNil(t, msgChan)

		time.Sleep(500 * time.Millisecond)

		conn, err := grpc.NewClient("localhost:50054",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(10*1024*1024),
				grpc.MaxCallSendMsgSize(10*1024*1024),
			),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := proto.NewEventBridgeServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		uuid := make([]byte, 16)
		_, _ = rand.Read(uuid)

		msg := &proto.EventMessage{
			Uuid:     uuid,
			Metadata: map[string]string{"test": "reply"},
			Data:     []byte("request data"),
		}

		go func() {
			select {
			case runnerMsg := <-msgChan:
				err := runnerMsg.Ack(&message.ReplyData{
					Data:     []byte("reply data"),
					Metadata: map[string]string{"reply": "yes"},
				})
				assert.NoError(t, err)
			case <-ctx.Done():
				t.Error("timeout waiting for message")
			}
		}()

		resp, err := client.SendEvent(ctx, msg)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.True(t, resp.Success)
	})
}

func TestGRPCSource_StreamEvents(t *testing.T) {
	cfg := &SourceConfig{
		Address:               "localhost:50052",
		MaxReceiveMessageSize: 10 * 1024 * 1024,
	}
	source, err := NewSource(cfg)
	require.NoError(t, err)
	defer source.Close()

	msgChan, err := source.Produce(10)
	require.NoError(t, err)
	require.NotNil(t, msgChan)

	time.Sleep(500 * time.Millisecond)

	conn, err := grpc.NewClient("localhost:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(10*1024*1024),
			grpc.MaxCallSendMsgSize(10*1024*1024),
		),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := proto.NewEventBridgeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamEvents(ctx)
	require.NoError(t, err)

	messageCount := 3
	ackCount := 0
	go func() {
		for {
			select {
			case runnerMsg := <-msgChan:
				if runnerMsg == nil {
					return
				}
				err := runnerMsg.Ack(nil)
				assert.NoError(t, err)
				ackCount++
			case <-ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < messageCount; i++ {
		uuid := make([]byte, 16)
		_, _ = rand.Read(uuid)

		msg := &proto.EventMessage{
			Uuid: uuid,
			Metadata: map[string]string{
				"index": string(rune(i)),
			},
			Data: []byte("stream data " + string(rune(i))),
		}

		err := stream.Send(msg)
		require.NoError(t, err)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Success)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, messageCount, ackCount)
}

func TestGRPCMessage(t *testing.T) {
	uuid := []byte("test-uuid")
	metadata := map[string]string{"key": "value"}
	data := []byte("test data")

	protoMsg := &proto.EventMessage{
		Uuid:     uuid,
		Metadata: metadata,
		Data:     data,
	}

	msg := NewGRPCMessage(protoMsg)
	require.NotNil(t, msg)

	t.Run("GetID", func(t *testing.T) {
		id := msg.GetID()
		assert.Equal(t, uuid, id)
	})

	t.Run("GetMetadata", func(t *testing.T) {
		meta, err := msg.GetMetadata()
		require.NoError(t, err)
		assert.Equal(t, metadata, meta)
	})

	t.Run("GetData", func(t *testing.T) {
		d, err := msg.GetData()
		require.NoError(t, err)
		assert.Equal(t, data, d)
	})

	t.Run("Ack", func(t *testing.T) {
		msg := NewGRPCMessage(protoMsg)
		err := msg.Ack(nil)
		require.NoError(t, err)

		select {
		case status := <-msg.done:
			assert.Equal(t, message.ResponseStatusAck, status)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for ack")
		}
	})

	t.Run("Ack with reply", func(t *testing.T) {
		msg := NewGRPCMessage(protoMsg)
		replyData := &message.ReplyData{
			Data:     []byte("reply"),
			Metadata: map[string]string{"reply": "yes"},
		}
		err := msg.Ack(replyData)
		require.NoError(t, err)

		select {
		case reply := <-msg.reply:
			assert.Equal(t, replyData, reply)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for reply")
		}
	})

	t.Run("Nak", func(t *testing.T) {
		msg := NewGRPCMessage(protoMsg)
		err := msg.Nak()
		require.NoError(t, err)

		select {
		case status := <-msg.done:
			assert.Equal(t, message.ResponseStatusNak, status)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for nak")
		}
	})

	t.Run("multiple calls use once", func(t *testing.T) {
		msg := NewGRPCMessage(protoMsg)
		err := msg.Ack(nil)
		require.NoError(t, err)

		err = msg.Ack(nil)
		require.NoError(t, err)

		select {
		case <-msg.done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timeout waiting for first ack")
		}

		select {
		case <-msg.done:
			t.Fatal("unexpected second signal")
		case <-time.After(100 * time.Millisecond):
		}
	})
}
