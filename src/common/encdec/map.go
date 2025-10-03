package encdec

import (
	"encoding/base64"
	"fmt"

	"github.com/sandrolain/events-bridge/src/common"
	"github.com/sandrolain/events-bridge/src/message"
)

func convertToStringMap(value any) (map[string]string, error) {
	switch typed := value.(type) {
	case map[string]string:
		return common.CopyMap(typed, nil), nil
	case map[string]any:
		return convertMapStringAny(typed)
	case map[any]any:
		return convertMapInterfaceAny(typed)
	case nil:
		return make(map[string]string), nil
	default:
		return nil, fmt.Errorf("metadata must be map[string]string (got %T)", value)
	}
}

func convertMapStringAny(src map[string]any) (map[string]string, error) {
	res := make(map[string]string, len(src))
	for k, v := range src {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("metadata value for key %q is not string (got %T)", k, v)
		}
		res[k] = str
	}
	return res, nil
}

func convertMapInterfaceAny(src map[any]any) (map[string]string, error) {
	res := make(map[string]string, len(src))
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
		if decoded, err := base64.StdEncoding.DecodeString(d); err == nil {
			data = decoded
		} else {
			data = []byte(d)
		}
	case []byte:
		data = d
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		data = []byte(fmt.Sprintf("%v", d))
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
