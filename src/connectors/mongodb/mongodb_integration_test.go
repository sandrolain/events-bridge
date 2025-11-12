//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	testDatabase   = "testdb"
	testCollection = "testcollection"
	mongoImage     = "mongo:7"
	replicaSetName = "rs0"
)

var (
	mongoContainer testcontainers.Container
	mongoURI       string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup MongoDB container with replica set (required for change streams)
	req := testcontainers.ContainerRequest{
		Image:        mongoImage,
		ExposedPorts: []string{"27017/tcp"},
		Cmd: []string{
			"--replSet", replicaSetName,
			"--bind_ip_all",
		},
		WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
	}

	mongoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to start MongoDB container: %v", err))
	}
	mongoContainer = mongoC

	// Get MongoDB URI
	host, err := mongoC.Host(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to get MongoDB host: %v", err))
	}
	port, err := mongoC.MappedPort(ctx, "27017/tcp")
	if err != nil {
		panic(fmt.Sprintf("failed to get MongoDB port: %v", err))
	}

	// Wait a bit for MongoDB to be fully ready
	time.Sleep(2 * time.Second)

	// Initialize replica set using direct connection
	if err := initializeReplicaSet(ctx, host, port.Port()); err != nil {
		panic(fmt.Sprintf("failed to initialize replica set: %v", err))
	}

	// Now connect with replica set URI - use directConnection to avoid DNS resolution
	mongoURI = fmt.Sprintf("mongodb://%s:%s/?directConnection=true&replicaSet=%s", host, port.Port(), replicaSetName)

	// Wait additional time for replica set to stabilize
	time.Sleep(3 * time.Second)

	// Verify connectivity
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 10*time.Second)
	defer verifyCancel()
	testClient, err := mongo.Connect(verifyCtx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(fmt.Sprintf("failed to connect to MongoDB for verification: %v", err))
	}
	if err := testClient.Ping(verifyCtx, nil); err != nil {
		testClient.Disconnect(verifyCtx)
		panic(fmt.Sprintf("failed to ping MongoDB after replica set init: %v", err))
	}
	testClient.Disconnect(verifyCtx)
	fmt.Println("✓ MongoDB replica set is ready and accessible")

	// Run tests
	code := m.Run()

	// Cleanup
	if err := mongoContainer.Terminate(ctx); err != nil {
		fmt.Printf("failed to terminate MongoDB container: %v\n", err)
	}

	os.Exit(code)
}

func initializeReplicaSet(ctx context.Context, host, port string) error {
	// Connect with direct connection to initialize replica set
	directURI := fmt.Sprintf("mongodb://%s:%s/?directConnection=true", host, port)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(directURI))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Check if replica set is already initialized
	var status bson.M
	err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
	if err == nil {
		// Replica set already initialized, check if primary exists
		if members, ok := status["members"].(primitive.A); ok {
			for _, m := range members {
				if member, ok := m.(bson.M); ok {
					if stateStr, ok := member["stateStr"].(string); ok && stateStr == "PRIMARY" {
						fmt.Println("Replica set already initialized with primary")
						return nil
					}
				}
			}
		}
	}

	// Initialize replica set with localhost (internal hostname)
	// MongoDB replica set must use the hostname that the server knows itself as
	config := bson.D{
		{Key: "replSetInitiate", Value: bson.D{
			{Key: "_id", Value: replicaSetName},
			{Key: "members", Value: bson.A{
				bson.D{
					{Key: "_id", Value: 0},
					{Key: "host", Value: "localhost:27017"},
				},
			}},
		}},
	}

	var result bson.M
	err = client.Database("admin").RunCommand(ctx, config).Decode(&result)
	if err != nil {
		// Check if error is because replica set already initiated
		if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.Code == 23 { // AlreadyInitialized
			fmt.Println("Replica set already initiated")
			return nil
		}
		return fmt.Errorf("failed to initialize replica set: %w", err)
	}

	fmt.Println("Replica set initiation command sent, waiting for primary election...")

	// Wait for replica set to elect a primary
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		time.Sleep(1 * time.Second)

		// Check replica set status
		status = bson.M{}
		err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
		if err != nil {
			// Might still be initializing
			continue
		}

		// Check if there's a primary
		if members, ok := status["members"].(primitive.A); ok {
			for _, m := range members {
				if member, ok := m.(bson.M); ok {
					if stateStr, ok := member["stateStr"].(string); ok && stateStr == "PRIMARY" {
						fmt.Printf("✓ Replica set initialized and primary elected after %d seconds\n", i+1)
						return nil
					}
				}
			}
		}
	}

	return fmt.Errorf("replica set initialization timeout - no primary elected after %d seconds", maxRetries)
}

func TestMongoDBInsertIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source configuration
	sourceCfg := &SourceConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		FullDocument:     "updateLookup",
		StrictValidation: false, // localhost is not strict
		ConnectTimeout:   30 * time.Second,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait a bit for the change stream to be ready
	time.Sleep(2 * time.Second)

	// Setup target configuration
	targetCfg := &RunnerConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		Operation:        "insert",
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
		OperationTimeout: 30 * time.Second,
	}

	// Create target
	target, err := NewRunner(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Insert test document via target
	testData := map[string]any{
		"name":   "integration test",
		"value":  42,
		"status": "active",
	}
	testDataBytes, err := json.Marshal(testData)
	require.NoError(t, err)

	testID := []byte("insert-test-id")
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:       testID,
		data:     testDataBytes,
		metadata: map[string]string{"test-type": "insert"},
	})

	err = target.Process(testMsg)
	require.NoError(t, err)

	// Wait for change stream event
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		// Parse change event
		var changeEvent map[string]any
		err = json.Unmarshal(data, &changeEvent)
		require.NoError(t, err)

		// Verify operation type
		assert.Equal(t, "insert", changeEvent["operationType"])

		// Verify full document contains our data
		fullDoc, ok := changeEvent["fullDocument"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "integration test", fullDoc["name"])
		assert.Equal(t, float64(42), fullDoc["value"])
		assert.Equal(t, "active", fullDoc["status"])

		// Verify metadata
		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, "insert", metadata["operationType"])
		assert.Equal(t, testDatabase, metadata["database"])
		assert.Equal(t, testCollection, metadata["collection"])

		// Acknowledge message
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for insert change event")
	}
}

func TestMongoDBUpdateIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup MongoDB client to insert initial document
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	collection := client.Database(testDatabase).Collection(testCollection)

	// Insert initial document
	initialDoc := bson.M{
		"name":   "update test",
		"value":  10,
		"status": "pending",
	}
	insertResult, err := collection.InsertOne(ctx, initialDoc)
	require.NoError(t, err)
	docID := insertResult.InsertedID.(primitive.ObjectID)

	// Setup source configuration
	sourceCfg := &SourceConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		FullDocument:     "updateLookup",
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait for change stream to be ready
	time.Sleep(2 * time.Second)

	// Setup target configuration for update
	filterJSON := fmt.Sprintf(`{"_id": {"$oid": "%s"}}`, docID.Hex())
	targetCfg := &RunnerConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		Operation:        "update",
		Filter:           filterJSON,
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
		OperationTimeout: 30 * time.Second,
	}

	// Create target
	target, err := NewRunner(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Update document via target
	updateData := map[string]any{
		"value":  100,
		"status": "completed",
	}
	updateDataBytes, err := json.Marshal(updateData)
	require.NoError(t, err)

	testMsg := message.NewRunnerMessage(&TestMessage{
		id:       []byte("update-test-id"),
		data:     updateDataBytes,
		metadata: map[string]string{"test-type": "update"},
	})

	err = target.Process(testMsg)
	require.NoError(t, err)

	// Wait for change stream event
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		var changeEvent map[string]any
		err = json.Unmarshal(data, &changeEvent)
		require.NoError(t, err)

		// Verify operation type
		assert.Equal(t, "update", changeEvent["operationType"])

		// Verify update description
		updateDesc, ok := changeEvent["updateDescription"].(map[string]any)
		require.True(t, ok)

		updatedFields, ok := updateDesc["updatedFields"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, float64(100), updatedFields["value"])
		assert.Equal(t, "completed", updatedFields["status"])

		// Acknowledge message
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for update change event")
	}
}

func TestMongoDBReplaceIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup MongoDB client to insert initial document
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	collection := client.Database(testDatabase).Collection(testCollection)

	// Insert initial document
	initialDoc := bson.M{
		"name":   "replace test",
		"value":  50,
		"status": "old",
	}
	insertResult, err := collection.InsertOne(ctx, initialDoc)
	require.NoError(t, err)
	docID := insertResult.InsertedID.(primitive.ObjectID)

	// Setup source configuration
	sourceCfg := &SourceConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		FullDocument:     "updateLookup",
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait for change stream to be ready
	time.Sleep(2 * time.Second)

	// Setup target configuration for replace
	filterJSON := fmt.Sprintf(`{"_id": {"$oid": "%s"}}`, docID.Hex())
	targetCfg := &RunnerConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		Operation:        "replace",
		Filter:           filterJSON,
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
		OperationTimeout: 30 * time.Second,
	}

	// Create target
	target, err := NewRunner(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Replace document via target
	replaceData := map[string]any{
		"name":        "replaced document",
		"newField":    "new value",
		"totallyNew":  true,
		"replaceTest": 123,
	}
	replaceDataBytes, err := json.Marshal(replaceData)
	require.NoError(t, err)

	testMsg := message.NewRunnerMessage(&TestMessage{
		id:       []byte("replace-test-id"),
		data:     replaceDataBytes,
		metadata: map[string]string{"test-type": "replace"},
	})

	err = target.Process(testMsg)
	require.NoError(t, err)

	// Wait for change stream event
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		var changeEvent map[string]any
		err = json.Unmarshal(data, &changeEvent)
		require.NoError(t, err)

		// Verify operation type (replace shows as "replace" in change stream)
		assert.Equal(t, "replace", changeEvent["operationType"])

		// Verify full document has new fields
		fullDoc, ok := changeEvent["fullDocument"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "replaced document", fullDoc["name"])
		assert.Equal(t, "new value", fullDoc["newField"])
		assert.Equal(t, true, fullDoc["totallyNew"])
		assert.Equal(t, float64(123), fullDoc["replaceTest"])

		// Acknowledge message
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for replace change event")
	}
}

func TestMongoDBDeleteIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup MongoDB client to insert document to delete
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	collection := client.Database(testDatabase).Collection(testCollection)

	// Insert document to delete
	docToDelete := bson.M{
		"name":   "delete test",
		"value":  999,
		"status": "to_be_deleted",
	}
	insertResult, err := collection.InsertOne(ctx, docToDelete)
	require.NoError(t, err)
	docID := insertResult.InsertedID.(primitive.ObjectID)

	// Setup source configuration
	sourceCfg := &SourceConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		FullDocument:     "updateLookup",
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait for change stream to be ready
	time.Sleep(2 * time.Second)

	// Setup target configuration for delete
	filterJSON := fmt.Sprintf(`{"_id": {"$oid": "%s"}}`, docID.Hex())
	targetCfg := &RunnerConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		Operation:        "delete",
		Filter:           filterJSON,
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
		OperationTimeout: 30 * time.Second,
	}

	// Create target
	target, err := NewRunner(targetCfg)
	require.NoError(t, err)
	defer target.Close()

	// Delete document via target
	testMsg := message.NewRunnerMessage(&TestMessage{
		id:       []byte("delete-test-id"),
		data:     []byte(`{}`),
		metadata: map[string]string{"test-type": "delete"},
	})

	err = target.Process(testMsg)
	require.NoError(t, err)

	// Wait for change stream event
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		var changeEvent map[string]any
		err = json.Unmarshal(data, &changeEvent)
		require.NoError(t, err)

		// Verify operation type
		assert.Equal(t, "delete", changeEvent["operationType"])

		// Verify document key
		metadata, err := receivedMsg.GetSourceMetadata()
		require.NoError(t, err)
		assert.Equal(t, "delete", metadata["operationType"])
		assert.Equal(t, testDatabase, metadata["database"])
		assert.Equal(t, testCollection, metadata["collection"])
		assert.Equal(t, docID.Hex(), metadata["documentId"])

		// Acknowledge message
		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-ctx.Done():
		t.Fatal("timeout waiting for delete change event")
	}
}

func TestMongoDBPipelineFilterIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Setup source with pipeline filter (only insert operations)
	sourceCfg := &SourceConfig{
		URI:              mongoURI,
		Database:         testDatabase,
		Collection:       testCollection,
		FullDocument:     "updateLookup",
		StrictValidation: false,
		ConnectTimeout:   30 * time.Second,
		Pipeline: []bson.M{
			{"$match": bson.M{"operationType": "insert"}},
		},
	}

	// Create source
	source, err := NewSource(sourceCfg)
	require.NoError(t, err)
	defer source.Close()

	// Start consuming messages
	msgChan, err := source.Produce(10)
	require.NoError(t, err)

	// Wait for change stream to be ready
	time.Sleep(2 * time.Second)

	// Setup MongoDB client for operations
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)
	defer client.Disconnect(ctx)

	collection := client.Database(testDatabase).Collection(testCollection)

	// Insert a document (should be captured)
	insertDoc := bson.M{
		"name":   "pipeline test insert",
		"filter": "should_pass",
	}
	_, err = collection.InsertOne(ctx, insertDoc)
	require.NoError(t, err)

	// Wait for insert event
	select {
	case receivedMsg := <-msgChan:
		data, err := receivedMsg.GetSourceData()
		require.NoError(t, err)

		var changeEvent map[string]any
		err = json.Unmarshal(data, &changeEvent)
		require.NoError(t, err)

		// Should only receive insert
		assert.Equal(t, "insert", changeEvent["operationType"])

		err = receivedMsg.Ack(nil)
		assert.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for filtered insert event")
	}

	// Update the document (should NOT be captured due to filter)
	_, err = collection.UpdateOne(ctx, bson.M{"name": "pipeline test insert"}, bson.M{"$set": bson.M{"filter": "updated"}})
	require.NoError(t, err)

	// Wait a bit to ensure update event doesn't arrive
	select {
	case receivedMsg := <-msgChan:
		data, _ := receivedMsg.GetSourceData()
		t.Fatalf("should not receive update event with pipeline filter, got: %s", string(data))
	case <-time.After(3 * time.Second):
		// Expected - update should be filtered out
	}
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
