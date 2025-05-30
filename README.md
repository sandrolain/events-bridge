# events-bridge

Events Bridge è un'applicazione scritta in Go per l'integrazione di eventi tra diverse sorgenti e destinazioni (HTTP, MQTT, NATS, CoAP, ecc.).

## Funzionalità principali

- Bridge di eventi tra protocolli diversi
- Configurazione tramite file YAML
- Supporto per estensioni tramite runner (WASM, ES5)

## Struttura del progetto

- `src/` — Codice sorgente principale
- `localtest/` — Ambiente di test locale (docker-compose, configurazioni)
- `testers/` — Tool di test per le varie integrazioni
- `tmp/` — Esempi e file temporanei

## Avvio rapido

1. Clona il repository
2. Modifica la configurazione in `tmp/config.yaml`
3. Avvia l'applicazione:

   ```sh
   go run src/main.go
   ```

## Requisiti

- Go 1.20+
- Docker (per test locali)

## Licenza

Vedi file LICENSE.
