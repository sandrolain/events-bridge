# events-bridge

Events Bridge is a Go application for integrating events across different sources and targets (HTTP, MQTT, NATS, CoAP, etc.).

## Key Features

- Bridge events across different protocols
- Configure via YAML files
- Extensible via runners (WASM, ES5)

## Project Structure

- `src/` — Main source code
- `localtest/` — Local testing environment (docker-compose, configurations)
- `testers/` — Testing tools for various integrations
- `tmp/` — Examples and temporary files

## Quick Start

1. Clone the repository
2. Edit the configuration in `tmp/config.yaml`
3. Start the application:

   ```sh
   go run src/main.go
   ```

## Requirements

- Go 1.20+
- Docker (for local tests)

## License

See the LICENSE file.
