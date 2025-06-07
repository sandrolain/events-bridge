package clirunner

import (
	"bytes"
	"errors"
	"io"
	"net/url"
)

// Encode serializza metadati e dati binari nel formato richiesto:
// querystring(metadati) + "\n" + dati binari
func Encode(metadata map[string][]string, data []byte) []byte {
	q := url.Values{}
	for k, vals := range metadata {
		for _, v := range vals {
			q.Add(k, v)
		}
	}
	var buf bytes.Buffer
	buf.WriteString(q.Encode())
	buf.WriteByte('\n')
	buf.Write(data)
	return buf.Bytes()
}

// Decode estrae metadati e dati binari dal formato:
// querystring(metadati) + "\n" + dati binari
func Decode(input []byte) (map[string][]string, []byte, error) {
	i := bytes.IndexByte(input, '\n')
	if i < 0 {
		return nil, nil, errors.New("invalid format: missing newline separator")
	}
	metaStr := string(input[:i])
	data := input[i+1:]
	m, err := url.ParseQuery(metaStr)
	if err != nil {
		return nil, nil, err
	}
	return m, data, nil
}

// DecodeFromReader permette di decodificare da uno stream (es. os.Stdin)
func DecodeFromReader(r io.Reader) (map[string][]string, []byte, error) {
	var metaBuf bytes.Buffer
	for {
		b := make([]byte, 1)
		n, err := r.Read(b)
		if n == 1 {
			if b[0] == '\n' {
				break
			}
			metaBuf.WriteByte(b[0])
		} else if err != nil {
			return nil, nil, err
		}
	}
	metaStr := metaBuf.String()
	m, err := url.ParseQuery(metaStr)
	if err != nil {
		return nil, nil, err
	}
	// Leggi il resto come dati binari
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}
	return m, data, nil
}
