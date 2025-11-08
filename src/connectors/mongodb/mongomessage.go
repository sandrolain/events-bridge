package main

import (
	"encoding/json"
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MongoMessage wraps a MongoDB change stream event
type MongoMessage struct {
	event bson.M
}

// GetData returns the JSON representation of the MongoDB change event
func (m *MongoMessage) GetData() ([]byte, error) {
	// Convert BSON to JSON
	data, err := json.Marshal(m.event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal MongoDB event: %w", err)
	}
	return data, nil
}

// GetMetadata extracts metadata from the MongoDB change event
func (m *MongoMessage) GetMetadata() (map[string]string, error) {
	metadata := make(map[string]string)

	// Extract operation type
	if opType, ok := m.event["operationType"].(string); ok {
		metadata["operationType"] = opType
	}

	// Extract namespace (database and collection)
	if ns, ok := m.event["ns"].(bson.M); ok {
		if db, ok := ns["db"].(string); ok {
			metadata["database"] = db
		}
		if coll, ok := ns["coll"].(string); ok {
			metadata["collection"] = coll
		}
	}

	// Extract cluster time
	if clusterTime, ok := m.event["clusterTime"].(primitive.Timestamp); ok {
		metadata["clusterTime"] = fmt.Sprintf("%d", clusterTime.T)
	}

	// Extract document key if available
	if docKey, ok := m.event["documentKey"].(bson.M); ok {
		if id, exists := docKey["_id"]; exists {
			// Handle ObjectID specially to extract the hex string
			if oid, ok := id.(primitive.ObjectID); ok {
				metadata["documentId"] = oid.Hex()
			} else {
				metadata["documentId"] = fmt.Sprintf("%v", id)
			}
		}
	}

	// Extract update description for update operations
	if updateDesc, ok := m.event["updateDescription"].(bson.M); ok {
		if updatedFields, ok := updateDesc["updatedFields"].(bson.M); ok {
			updatedFieldsJSON, err := json.Marshal(updatedFields)
			if err == nil {
				metadata["updatedFields"] = string(updatedFieldsJSON)
			}
		}
		if removedFields, ok := updateDesc["removedFields"].([]any); ok {
			removedFieldsJSON, err := json.Marshal(removedFields)
			if err == nil {
				metadata["removedFields"] = string(removedFieldsJSON)
			}
		}
	}

	return metadata, nil
}

// Ack acknowledges the message (no-op for MongoDB change streams)
func (m *MongoMessage) Ack() error {
	return nil
}

// GetID returns the message ID from the change stream event
func (m *MongoMessage) GetID() []byte {
	if id, ok := m.event["_id"]; ok {
		return []byte(fmt.Sprintf("%v", id))
	}
	return []byte("mongodb-event")
}

// Nak negatively acknowledges the message (no-op for MongoDB change streams)
func (m *MongoMessage) Nak() error {
	return nil
}

// Reply sends a reply (no-op for MongoDB change streams)
func (m *MongoMessage) Reply(data *message.ReplyData) error {
	return nil
}
