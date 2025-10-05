# Events Bridge

A flexible, plugin-based event streaming platform for integrating and transforming messages across different protocols and data sources.

## Overview

Events Bridge enables you to:

- **Consume** events from a source (HTTP, MQTT, NATS, Kafka, Redis, PostgreSQL, CoAP, Git, etc.)
- **Transform** messages through one or more runners (WASM, ES5, Expr, JSONLogic, GPT)
- **Publish** events to a target using the same variety of protocols

All through a simple YAML configuration file.

## Architecture

The application follows a **Source → Runner(s) → Target** pipeline:

- **Source**: Produces messages from a data source (connector plugins)
- **Runner** (optional): Transforms, filters, or enriches messages (can chain multiple runners)
- **Target**: Consumes and publishes messages to a destination (connector plugins)

### Plugin-Based Architecture

Events Bridge uses a **dynamic plugin system** for connectors:

- **Connectors are compiled as Go plugins** (`.so` files) and loaded at runtime
- Each connector implements one or more interfaces: `Source`, `Runner`, or `Target`
- Plugins are discovered automatically from the `bin/connectors/` directory
- Configuration is passed to plugins via the `options` map in YAML
- This architecture allows:
  - **Easy extensibility**: Add new protocols without recompiling the main binary
  - **Hot-swappable connectors**: Update plugins independently
  - **Clean separation**: Each protocol/service lives in its own module

**Plugin Interfaces:**

```go
// Source produces messages
type Source interface {
    Produce(buffer int) (<-chan *message.RunnerMessage, error)
    Close() error
}

// Runner transforms messages
type Runner interface {
    Process(*message.RunnerMessage) error
    Close() error
}

// Target consumes messages
type Target interface {
    Consume(*message.RunnerMessage) error
    Close() error
}
```

Each plugin exports factory functions (`NewSource`, `NewRunner`, `NewTarget`) that accept configuration and return the appropriate interface implementation.

## Supported Connectors

### Sources & Targets

- **HTTP/HTTPS**: REST APIs and webhooks
- **MQTT**: IoT messaging protocol
- **NATS**: Cloud-native messaging system
- **Kafka**: Distributed event streaming
- **Redis**: Streams and Pub/Sub
- **PostgreSQL**: Database polling and LISTEN/NOTIFY
- **CoAP**: Constrained Application Protocol
- **Google Pub/Sub**: Cloud messaging
- **Git**: Repository monitoring
- **CLI**: Command-line input/output

### Runners

- **WASM**: WebAssembly modules for custom logic
- **ES5**: JavaScript transformation (Goja engine)
- **Expr**: Expression language for filtering and transformations
- **JSONLogic**: JSON-based logic rules
- **GPT**: OpenAI integration for AI-powered processing
- **Plugin**: Custom Go plugins

## Configuration

Create a YAML configuration file:

```yaml
source:
  type: "http"
  buffer: 100
  options:
    port: 8080
    path: "/webhook"

runners:
  - type: "expr"
    options:
      expr: "data.value * 2"
  
  - type: "wasm"
    options:
      wasmFile: "./transform.wasm"

target:
  type: "mqtt"
  routines: 5
  options:
    broker: "tcp://localhost:1883"
    topic: "events/processed"
```

Set the config path via environment:

```sh
export EB_CONFIG_FILE_PATH=/path/to/config.yaml
```

Or provide configuration directly:

```sh
export EB_CONFIG_CONTENT="$(cat config.yaml)"
export EB_CONFIG_FORMAT=yaml
```

## Installation

### Build from source

```sh
go build -o events-bridge ./src
```

### Build with Task (recommended)

```sh
task build
```

This creates the main binary and all connector plugins in `bin/`.

## Usage

```sh
./events-bridge
```

Or run directly:

```sh
go run ./src/main.go
```

## Development

### Requirements

- Go 1.25+
- Docker (for integration tests)
- Task (optional, for build automation)

### Available Tasks

```sh
task                    # Run all checks (fmt, lint, vet, test)
task build              # Build binaries and plugins
task test               # Run all tests
task test-coverage      # Generate coverage report
task security-scan      # Run security analysis
```

### Project Structure

```text
src/
  ├── main.go           # Application entry point
  ├── config/           # Configuration loading and validation
  ├── connectors/       # Plugin system and connector interfaces
  ├── message/          # Message types and utilities
  ├── common/           # Shared utilities (encoding, expressions, etc.)
  └── utils/            # Plugin loader and helpers

testers/                # Integration testing tools
localtest/              # Docker compose environments
```

## Testing

Run tests:

```sh
go test ./...
```

With coverage:

```sh
task test-coverage
```

Integration tests use Testcontainers for spinning up dependencies (Kafka, Redis, PostgreSQL, etc.).

## License

See the [LICENSE](LICENSE) file for details.
