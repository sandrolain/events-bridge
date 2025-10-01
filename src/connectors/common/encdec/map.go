package encdec

import (
	"fmt"

	"github.com/sandrolain/events-bridge/src/message"
)

func mapToMessage(v map[string]any, metaKey string, dataKey string) (message.SourceMessage, error) {
	m := v[metaKey]
	d := v[dataKey]
	var meta message.MessageMetadata = make(message.MessageMetadata)
	var data []byte

	switch m.(type) {
	case map[string]any:
		cm := m.(map[string]any)
		for k, val := range cm {
			if strVal, ok := val.(string); ok {
				meta[k] = strVal
			} else {
				meta[k] = fmt.Sprintf("%v", val)
			}
		}
	case map[string]string:
		meta = m.(map[string]string)
	case map[interface{}]interface{}:
		cm := m.(map[interface{}]interface{})
		for k, val := range cm {
			if keyStr, ok := k.(string); ok {
				if strVal, ok := val.(string); ok {
					meta[keyStr] = strVal
				} else {
					meta[keyStr] = fmt.Sprintf("%v", val)
				}
			}
		}
	case nil:
		// no metadata
	default:
		return nil, fmt.Errorf("invalid metadata type: %T", m)
	}

	switch d.(type) {
	case string:
		data = []byte(d.(string))
	case []byte:
		data = d.([]byte)
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
