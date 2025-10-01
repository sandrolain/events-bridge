package encdec

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
)

func convertToStringMap(value any) (message.MessageMetadata, error) {
	switch typed := value.(type) {
	case map[string]string:
		return copyStringStringMap(typed), nil
	case map[string]any:
		return convertMapStringAny(typed)
	case map[any]any:
		return convertMapInterfaceAny(typed)
	case nil:
		return make(message.MessageMetadata), nil
	default:
		return nil, fmt.Errorf("metadata must be map[string]string (got %T)", value)
	}
}

func copyStringStringMap(src map[string]string) message.MessageMetadata {
	res := make(message.MessageMetadata, len(src))
	for k, v := range src {
		res[k] = v
	}
	return res
}

func convertMapStringAny(src map[string]any) (message.MessageMetadata, error) {
	res := make(message.MessageMetadata, len(src))
	for k, v := range src {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("metadata value for key %q is not string (got %T)", k, v)
		}
		res[k] = str
	}
	return res, nil
}

func convertMapInterfaceAny(src map[any]any) (message.MessageMetadata, error) {
	res := make(message.MessageMetadata, len(src))
	for k, v := range src {
		key, ok := k.(string)
		if !ok {
			return nil, fmt.Errorf("metadata key is not string (got %T)", k)
		}
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("metadata value for key %q is not string (got %T)", key, v)
		}
		res[key] = str
	}
	return res, nil
}

func mapToMessage(decoder MessageDecoder, v map[string]any, metaKey string, dataKey string) (message.SourceMessage, error) {
	m := v[metaKey]
	d := v[dataKey]

	meta, err := convertToStringMap(m)
	if err != nil {
		return nil, fmt.Errorf("failed to convert metadata: %w", err)
	}

	var data []byte

	switch d := d.(type) {
	case map[string]any, map[any]any:
		var err error
		data, err = decoder.Encode(d)
		if err != nil {
			return nil, fmt.Errorf("failed to encode JSON data: %w", err)
		}
	case string:
		data = []byte(d)
	case []byte:
		data = d
	case nil:
		// no data
	default:
		return nil, fmt.Errorf("invalid data type: %T", d)
	}

	return NewEncDecMessage(meta, data), nil
}

func messageToMap(msg message.SourceMessage, metaKey string, dataKey string) (map[string]any, error) {
	m, err := msg.GetMetadata()
	if err != nil {
		return nil, err
	}
	d, err := msg.GetData()
	if err != nil {
		return nil, err
	}
	v := map[string]any{
		metaKey: m,
		dataKey: d,
	}
	return v, nil
}
