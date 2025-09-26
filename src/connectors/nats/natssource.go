package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/connectors"
	"github.com/sandrolain/events-bridge/src/message"
)

type SourceConfig struct {
	Address    string `mapstructure:"address" validate:"required"`
	Subject    string `mapstructure:"subject" validate:"required"`
	Stream     string `mapstructure:"stream"`
	Consumer   string `mapstructure:"consumer"`
	QueueGroup string `mapstructure:"queueGroup"`
}

type NATSSource struct {
	cfg  *SourceConfig
	slog *slog.Logger
	c    chan *message.RunnerMessage
	nc   *nats.Conn
	js   nats.JetStreamContext
	sub  *nats.Subscription
}

func NewSourceConfig() any {
	return new(SourceConfig)
}

func NewSource(anyCfg any) (connectors.Source, error) {
	cfg, ok := anyCfg.(*SourceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}
	return &NATSSource{
		cfg:  cfg,
		slog: slog.Default().With("context", "NATS Source"),
	}, nil
}

func (s *NATSSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting NATS source", "address", s.cfg.Address, "subject", s.cfg.Subject, "stream", s.cfg.Stream, "consumer", s.cfg.Consumer, "queueGroup", s.cfg.QueueGroup)

	nc, err := nats.Connect(s.cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	s.nc = nc

	// If both stream and consumer are specified, use JetStream
	if s.cfg.Stream != "" && s.cfg.Consumer != "" {
		js, err := nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get JetStream context: %w", err)
		}
		s.js = js
		s.slog.Info("using JetStream", "stream", s.cfg.Stream, "consumer", s.cfg.Consumer)

		if err := s.consumeJetStream(); err != nil {
			return nil, fmt.Errorf("failed to start JetStream consumer: %w", err)
		}
	} else {
		// NATS core (o JetStream senza consumer)
		queue := s.cfg.QueueGroup
		if err := s.consumeCore(queue); err != nil {
			return nil, fmt.Errorf("failed to start NATS core consumer: %w", err)
		}
	}

	return s.c, nil
}

func (s *NATSSource) consumeCore(queue string) (err error) {
	handler := func(msg *nats.Msg) {
		m := &NATSMessage{
			msg: msg,
		}
		s.c <- message.NewRunnerMessage(m)
	}
	var e error
	if queue != "" {
		s.sub, e = s.nc.QueueSubscribe(s.cfg.Subject, queue, handler)
	} else {
		s.sub, e = s.nc.Subscribe(s.cfg.Subject, handler)
	}
	if e != nil {
		err = fmt.Errorf("failed to subscribe to subject: %w", e)
	}
	return
}

func (s *NATSSource) consumeJetStream() (err error) {
	js := s.js
	stream := s.cfg.Stream
	consumer := s.cfg.Consumer
	sub, e := js.PullSubscribe(s.cfg.Subject, consumer, nats.Bind(stream, consumer))
	if e != nil {
		err = fmt.Errorf("failed to create JetStream pull subscription: %w", e)
		return
	}
	s.sub = sub
	go func() {
		for {
			msgs, err := s.sub.Fetch(1, nats.MaxWait(5*time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					s.slog.Warn("JetStream fetch timeout")
					continue
				}
				s.slog.Error("error fetching from JetStream", "err", err)
				break
			}
			for _, msg := range msgs {
				m := &NATSMessage{
					msg: msg,
				}
				s.c <- message.NewRunnerMessage(m)
			}
		}
	}()
	return
}

func (s *NATSSource) Close() error {
	// Unsubscribe/Drain subscription before closing channel to avoid send-on-closed-channel
	if s.sub != nil {
		if err := s.sub.Drain(); err != nil {
			_ = s.sub.Unsubscribe()
		}
		s.sub = nil
	}
	if s.nc != nil {
		s.nc.Close()
		s.nc = nil
	}
	if s.c != nil {
		close(s.c)
		s.c = nil
	}
	return nil
}
