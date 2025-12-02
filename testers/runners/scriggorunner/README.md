# Scriggo Runner Test

This directory contains a test script for the Scriggo runner.

## Files

- `processor.scriggo` - Example Go script that processes messages using the Scriggo interpreter (uses `.scriggo` extension to avoid Go tooling conflicts)

## What the Script Does

The `processor.go` script:

1. Reads the incoming message data
2. Converts the text to uppercase
3. Adds a timestamp and processing information
4. Sets the processed data back to the message
5. Adds metadata about the processing

## Available Packages

The Scriggo runner exposes the following Go standard library packages:

| Package | Description |
|---------|-------------|
| `events` | Message access (GetData, SetData, AddMetadata, GetMetadata) |
| `fmt` | Formatting functions (Sprintf, Printf, Print, Println, Sprint, Sprintln, Errorf) |
| `strings` | String manipulation (ToUpper, ToLower, Contains, HasPrefix, HasSuffix, Split, Join, Replace, ReplaceAll, Trim, TrimSpace, TrimPrefix, TrimSuffix, Index, NewReader, NewReplacer) |
| `strconv` | Type conversion (Atoi, Itoa, FormatInt, FormatBool, ParseInt, ParseFloat, ParseBool) |
| `time` | Time functions (Now, Since, Until, Parse, ParseDuration, Date, Unix, UnixMilli, Sleep) and constants (RFC3339, RFC3339Nano, RFC1123, Second, Minute, Hour, etc.) |
| `bytes` | Byte slice manipulation (Buffer, NewBuffer, Contains, Equal, HasPrefix, HasSuffix, Index, Join, Split, ToLower, ToUpper, Trim, TrimSpace, TrimPrefix, TrimSuffix) |
| `encoding/json` | JSON encoding (Marshal, Unmarshal, MarshalIndent, Valid) |
| `encoding/base64` | Base64 encoding (StdEncoding, URLEncoding, RawStdEncoding, RawURLEncoding) |
| `math` | Math functions (Abs, Ceil, Floor, Max, Min, Pow, Round, Sqrt) |
| `sort` | Sorting functions (Strings, Ints, Float64s) |
| `regexp` | Regular expressions (Compile, MustCompile, Match, MatchString, QuoteMeta) |

## Example Processing

Input:

```
42 - 2025-11-26T20:45:00Z
```

Output:

```
[Scriggo Processed at 2025-11-26T20:45:01+01:00] 42 - 2025-11-26T20:45:00Z
```

Metadata added:

- `eb-scriggo-processed`: "true"
- `eb-scriggo-timestamp`: Processing timestamp
- `eb-scriggo-original-length`: Original data length
- `eb-scriggo-processed-length`: Processed data length
- `eb-scriggo-original-source`: Original source metadata (if present)

## Running the Test

From the project root:

```bash
task run:http-scriggo-http
```

Or using the testers tool:

```bash
./testers/bin/task-runner run testers/config/http-scriggo-http.yaml
```

## Configuration

The test configuration (`testers/config/http-scriggo-http.yaml`) sets up:

1. **HTTP Source**: Sends test payloads every second
2. **Scriggo Runner**: Processes messages using the Go script with 8 concurrent routines
3. **HTTP Target**: Receives the processed messages

## Script Features Demonstrated

- Reading message data with error handling
- Reading and checking metadata
- String manipulation (uppercase conversion)
- Time formatting
- Adding multiple metadata fields
- Panic handling for errors
