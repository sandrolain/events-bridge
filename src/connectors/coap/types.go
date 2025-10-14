package main

type CoAPProtocol string

const (
	CoAPProtocolUDP  CoAPProtocol = "udp"
	CoAPProtocolTCP  CoAPProtocol = "tcp"
	CoAPProtocolDTLS CoAPProtocol = "dtls"
)
