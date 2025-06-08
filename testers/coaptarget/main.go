package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	coap "github.com/plgd-dev/go-coap/v3"
	coapmessage "github.com/plgd-dev/go-coap/v3/message"
	coapcodes "github.com/plgd-dev/go-coap/v3/message/codes"
	coapmux "github.com/plgd-dev/go-coap/v3/mux"
)

func printRequestDetails(proto, remote string, req *coapmux.Message) {
	fmt.Println("\n----------------------------------------")
	fmt.Println(time.Now().Format(time.RFC3339))
	fmt.Printf("Request from %s (%s):\n", remote, proto)
	fmt.Printf("  Code: %v\n", req.Code())
	path, _ := req.Options().Path()
	fmt.Printf("  Path: %s\n", path)
	// Query string manual extraction
	var query string
	for _, opt := range req.Options() {
		if opt.ID == coapmessage.URIQuery {
			if query != "" {
				query += "&"
			}
			query += string(opt.Value)
		}
	}
	fmt.Printf("  Query: %s\n", query)
	fmt.Printf("  Token: %v\n", req.Token())
	fmt.Println("  Options:")
	for _, opt := range req.Options() {
		fmt.Printf("    %v: %v\n", opt.ID, opt.Value)
	}
	fmt.Println("  Payload:")
	if req.Body() != nil {
		buf := new(bytes.Buffer)
		buf.ReadFrom(req.Body())
		fmt.Printf("    %s\n", buf.String())
	} else {
		fmt.Println("    <empty>")
	}
}

func main() {
	mode := flag.String("mode", "udp", "CoAP protocol: udp or tcp")
	addr := flag.String("address", ":5683", "Listen address (e.g.: :5683)")
	flag.Parse()

	log.Printf("Starting coapdbg in %s mode on %s", *mode, *addr)

	router := coapmux.NewRouter()
	router.Handle("/", coapmux.HandlerFunc(func(w coapmux.ResponseWriter, req *coapmux.Message) {
		printRequestDetails(*mode, w.Conn().RemoteAddr().String(), req)
		w.SetResponse(coapcodes.Content, coapmessage.TextPlain, bytes.NewReader([]byte("OK")))
	}))

	switch *mode {
	case "udp", "tcp":
		err := coap.ListenAndServe(*mode, *addr, router)
		if err != nil {
			log.Fatalf("Serve error: %v", err)
		}
	default:
		log.Fatalf("Unknown mode: %s (use udp or tcp)", *mode)
	}
}
