package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestMongoMessage_GetData(t *testing.T) {
	event := bson.M{
		"_id": bson.M{
			"_data": "test-resume-token",
		},
		"operationType": "insert",
		"ns": bson.M{
			"db":   "testdb",
			"coll": "testcoll",
		},
		"documentKey": bson.M{
			"_id": primitive.NewObjectID(),
		},
		"fullDocument": bson.M{
			"name":  "test",
			"value": 123,
		},
	}

	msg := &MongoMessage{event: event}

	data, err := msg.GetData()
	require.NoError(t, err)
	assert.NotNil(t, data)
	assert.Greater(t, len(data), 0)

	// Verify it's valid JSON
	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "insert", result["operationType"])
}

func TestMongoMessage_GetMetadata(t *testing.T) {
	objectID := primitive.NewObjectID()
	timestamp := primitive.Timestamp{T: 1234567890, I: 1}

	event := bson.M{
		"operationType": "update",
		"ns": bson.M{
			"db":   "mydb",
			"coll": "mycoll",
		},
		"clusterTime": timestamp,
		"documentKey": bson.M{
			"_id": objectID,
		},
		"updateDescription": bson.M{
			"updatedFields": bson.M{
				"status": "active",
			},
			"removedFields": []any{"tempField"},
		},
	}

	msg := &MongoMessage{event: event}

	metadata, err := msg.GetMetadata()
	require.NoError(t, err)
	assert.NotNil(t, metadata)

	assert.Equal(t, "update", metadata["operationType"])
	assert.Equal(t, "mydb", metadata["database"])
	assert.Equal(t, "mycoll", metadata["collection"])
	assert.Equal(t, "1234567890", metadata["clusterTime"])
	assert.Equal(t, objectID.Hex(), metadata["documentId"])
	assert.Contains(t, metadata, "updatedFields")
	assert.Contains(t, metadata, "removedFields")
}

func TestMongoMessage_Ack(t *testing.T) {
	msg := &MongoMessage{event: bson.M{}}
	err := msg.Ack()
	assert.NoError(t, err)
}

func TestMongoMessage_EmptyEvent(t *testing.T) {
	msg := &MongoMessage{event: bson.M{}}

	data, err := msg.GetData()
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))

	metadata, err := msg.GetMetadata()
	require.NoError(t, err)
	assert.Empty(t, metadata)
}
