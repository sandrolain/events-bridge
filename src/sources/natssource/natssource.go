package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
)

type NATSSource struct {
	config  *sources.SourceNATSConfig
	slog    *slog.Logger
	c       chan message.Message
	nc      *nats.Conn
	js      nats.JetStreamContext
	started bool
}

func New(cfg *sources.SourceNATSConfig) (sources.Source, error) {
	if cfg.Address == "" || cfg.Subject == "" {
		return nil, fmt.Errorf("address and subject are required for NATS source")
	}
	return &NATSSource{
		config: cfg,
		slog:   slog.Default().With("context", "NATS"),
	}, nil
}

func (s *NATSSource) Produce(buffer int) (<-chan message.Message, error) {
	s.c = make(chan message.Message, buffer)

	s.slog.Info("starting NATS source", "address", s.config.Address, "subject", s.config.Subject, "stream", s.config.Stream, "consumer", s.config.Consumer, "queueGroup", s.config.QueueGroup)

	nc, err := nats.Connect(s.config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	s.nc = nc

	// Se stream e consumer sono specificati, usa JetStream
	if s.config.Stream != "" && s.config.Consumer != "" {
		js, err := nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get JetStream context: %w", err)
		}
		s.js = js
		s.slog.Info("using JetStream", "stream", s.config.Stream, "consumer", s.config.Consumer)
		go s.consumeJetStream()
	} else {
		// NATS core (o JetStream senza consumer)
		queue := s.config.QueueGroup
		go s.consumeCore(queue)
	}

	s.started = true
	return s.c, nil
}

func (s *NATSSource) consumeCore(queue string) {
	handler := func(msg *nats.Msg) {
		m := &NATSMessage{
			subject: msg.Subject,
			payload: msg.Data,
			msg:     msg,
		}
		s.c <- m
	}
	if queue != "" {
		s.nc.QueueSubscribe(s.config.Subject, queue, handler)
	} else {
		s.nc.Subscribe(s.config.Subject, handler)
	}
}

func (s *NATSSource) consumeJetStream() {
	js := s.js
	stream := s.config.Stream
	consumer := s.config.Consumer
	sub, err := js.PullSubscribe(s.config.Subject, consumer, nats.Bind(stream, consumer))
	if err != nil {
		s.slog.Error("failed to subscribe to JetStream", "err", err)
		return
	}
	for {
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			s.slog.Error("error fetching from JetStream", "err", err)
			break
		}
		for _, msg := range msgs {
			m := &NATSMessage{
				subject: msg.Subject,
				payload: msg.Data,
				msg:     msg,
			}
			s.c <- m
		}
	}
}

func (s *NATSSource) Close() error {
	if s.c != nil {
		close(s.c)
	}
	if s.nc != nil {
		s.nc.Close()
	}
	return nil
}
