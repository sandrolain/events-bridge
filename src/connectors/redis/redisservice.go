package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandrolain/events-bridge/src/connectors"
)

var _ connectors.Service = (*RedisService)(nil)

type ServiceConfig struct {
	Address  string        `mapstructure:"address" validate:"required"`
	Username string        `mapstructure:"username"`
	Password string        `mapstructure:"password"`
	Database int           `mapstructure:"database"`
	Timeout  time.Duration `mapstructure:"timeout" default:"5s" validate:"gt=0"`
}

func NewServiceConfig() any {
	return new(ServiceConfig)
}

func NewService(anyCfg any) (connectors.Service, error) {
	cfg, ok := anyCfg.(*ServiceConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type: %T", anyCfg)
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.Database,
	})

	logger := slog.Default().With("context", "Redis Service")
	logger.Info("Redis service connected", "address", cfg.Address)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	commands := client.CommandList(ctx, nil)
	if err := commands.Err(); err != nil {
		return nil, fmt.Errorf("error listing Redis commands: %w", err)
	}
	commandsList := commands.Val()
	cmdMap := make(map[string]struct{}, len(commandsList))
	for _, cmd := range commandsList {
		cmd = strings.ToLower(cmd)
		cmdMap[cmd] = struct{}{}
	}

	logger.Debug("Redis commands loaded", "commands", strings.Join(commandsList, ", "))

	return &RedisService{
		cfg:      cfg,
		logger:   logger,
		client:   client,
		commands: cmdMap,
	}, nil
}

type RedisService struct {
	cfg      *ServiceConfig
	logger   *slog.Logger
	client   *redis.Client
	commands map[string]struct{}
}

func (s *RedisService) List() ([]string, error) {
	cmds := make([]string, 0, len(s.commands))
	for cmd := range s.commands {
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

func (s *RedisService) IsValidMethod(method string, args []any) bool {
	method = strings.ToLower(method)
	_, ok := s.commands[method]
	return ok
}

func (s *RedisService) Call(command string, args []any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()

	cmdArgs := make([]any, 0, len(args)+1)
	cmdArgs = append(cmdArgs, command)
	cmdArgs = append(cmdArgs, args...)

	s.logger.Debug("executing Redis command", "command", command, "args", len(args))

	result, err := s.client.Do(ctx, cmdArgs...).Result()
	if err != nil {
		return nil, fmt.Errorf("error executing Redis command: %w", err)
	}

	data, err := convertToBytes(result)
	if err != nil {
		return nil, fmt.Errorf("error converting Redis response: %w", err)
	}

	s.logger.Debug("Redis command executed", "command", command, "responseSize", len(data))
	return data, nil
}

func (s *RedisService) Close() error {
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			return fmt.Errorf("error closing Redis client: %w", err)
		}
	}
	return nil
}

func convertToBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case fmt.Stringer:
		return []byte(v.String()), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case int:
		return []byte(strconv.Itoa(v)), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case []any, map[string]any:
		return json.Marshal(v)
	default:
		return json.Marshal(v)
	}
}
