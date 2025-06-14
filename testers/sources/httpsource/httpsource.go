package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	testpayload "github.com/sandrolain/events-bridge/testers/sources/testpayload"
)

func main() {
	address := flag.String("address", "http://localhost:8080", "HTTP server address (e.g. http://localhost:8080)")
	method := flag.String("method", "POST", "HTTP method (POST, PUT, PATCH)")
	path := flag.String("path", "/test", "HTTP request path")
	payload := flag.String("payload", "{}", "Payload to send (as text)")
	interval := flag.String("interval", "5s", "Intervallo tra le richieste (es. 2s, 500ms, 1m)")
	mime := flag.String("mime", "application/json", "Payload MIME type (e.g. application/json, text/plain, application/xml, ...)")
	testPayloadType := flag.String("testpayload", "", "If set, use testpayload generator: json, cbor, sentiment, sentence")
	flag.Parse()

	url := *address + *path

	dur, err := time.ParseDuration(*interval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Errore di parsing per interval: %v\n", err)
		os.Exit(1)
	}
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	fmt.Printf("Invio richieste %s a %s ogni %s\n", *method, url, dur)

	sendRequest := func() {
		var body io.Reader
		var contentType string

		if *testPayloadType != "" {
			switch *testPayloadType {
			case "json":
				b, _ := testpayload.GenerateRandomJSON()
				body = bytes.NewBuffer(b)
				contentType = "application/json"
			case "cbor":
				b, _ := testpayload.GenerateRandomCBOR()
				body = bytes.NewBuffer(b)
				contentType = "application/cbor"
			case "sentiment":
				str := testpayload.GenerateSentimentPhrase()
				body = bytes.NewBufferString(str)
				contentType = "text/plain"
			case "sentence":
				str := testpayload.GenerateSentence()
				body = bytes.NewBufferString(str)
				contentType = "text/plain"
			default:
				fmt.Fprintf(os.Stderr, "Unknown testpayload type: %s\n", *testPayloadType)
				return
			}
		} else if *payload != "" {
			body = bytes.NewBuffer([]byte(*payload))
			contentType = *mime
		}

		req, err := http.NewRequest(*method, url, body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Request creation error: %v\n", err)
			return
		}
		req.Header.Set("Content-Type", contentType)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
			return
		}
		fmt.Printf("Response: %d\n", resp.StatusCode)
		resp.Body.Close()
	}

	for range ticker.C {
		go sendRequest()
	}

	select {}
}
