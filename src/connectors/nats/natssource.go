package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/sources"
	"github.com/sandrolain/events-bridge/src/utils"
)

type SourceConfig struct {
	Address    string `yaml:"address" json:"address"`
	Stream     string `yaml:"stream" json:"stream"`
	Subject    string `yaml:"subject" json:"subject"`
	Consumer   string `yaml:"consumer" json:"consumer"`
	QueueGroup string `yaml:"queueGroup" json:"queueGroup"`
}

// parseSourceOptions builds a NATS source config from options map.
// Expected keys: address, subject, stream, consumer, queueGroup.
func parseSourceOptions(opts map[string]any) (*SourceConfig, error) {
	cfg := &SourceConfig{}
	op := &utils.OptsParser{}
	cfg.Address = op.OptString(opts, "address", "", utils.StringNonEmpty())
	cfg.Subject = op.OptString(opts, "subject", "", utils.StringNonEmpty())
	cfg.Stream = op.OptString(opts, "stream", "")
	cfg.Consumer = op.OptString(opts, "consumer", "")
	cfg.QueueGroup = op.OptString(opts, "queueGroup", "", utils.StringNonEmpty())
	if err := op.Error(); err != nil {
		return nil, err
	}
	return cfg, nil
}

type NATSSource struct {
	config  *SourceConfig
	slog    *slog.Logger
	c       chan *message.RunnerMessage
	nc      *nats.Conn
	js      nats.JetStreamContext
	sub     *nats.Subscription
	started bool
}

// NewSource creates the NATS source from options map.
func NewSource(opts map[string]any) (sources.Source, error) {
	cfg, err := parseSourceOptions(opts)
	if err != nil {
		return nil, err
	}
	return &NATSSource{
		config: cfg,
		slog:   slog.Default().With("context", "NATS"),
	}, nil
}

func (s *NATSSource) Produce(buffer int) (<-chan *message.RunnerMessage, error) {
	s.c = make(chan *message.RunnerMessage, buffer)

	s.slog.Info("starting NATS source", "address", s.config.Address, "subject", s.config.Subject, "stream", s.config.Stream, "consumer", s.config.Consumer, "queueGroup", s.config.QueueGroup)

	nc, err := nats.Connect(s.config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	s.nc = nc

	// If both stream and consumer are specified, use JetStream
	if s.config.Stream != "" && s.config.Consumer != "" {
		js, err := nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get JetStream context: %w", err)
		}
		s.js = js
		s.slog.Info("using JetStream", "stream", s.config.Stream, "consumer", s.config.Consumer)

		if err := s.consumeJetStream(); err != nil {
			return nil, fmt.Errorf("failed to start JetStream consumer: %w", err)
		}
	} else {
		// NATS core (o JetStream senza consumer)
		queue := s.config.QueueGroup
		if err := s.consumeCore(queue); err != nil {
			return nil, fmt.Errorf("failed to start NATS core consumer: %w", err)
		}
	}

	s.started = true
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
		s.sub, e = s.nc.QueueSubscribe(s.config.Subject, queue, handler)
	} else {
		s.sub, e = s.nc.Subscribe(s.config.Subject, handler)
	}
	if e != nil {
		err = fmt.Errorf("failed to subscribe to subject: %w", e)
	}
	return
}

func (s *NATSSource) consumeJetStream() (err error) {
	js := s.js
	stream := s.config.Stream
	consumer := s.config.Consumer
	sub, e := js.PullSubscribe(s.config.Subject, consumer, nats.Bind(stream, consumer))
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
