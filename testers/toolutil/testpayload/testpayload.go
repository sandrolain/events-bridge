package testpayload

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-faker/faker/v4"
)

// Payload represents the predictable payload structure
// faker annotates fields for automatic generation
// https://github.com/go-faker/faker#supported-tags
type Payload struct {
	ID     string  `faker:"uuid_hyphenated" json:"id"`
	Name   string  `faker:"name" json:"name"`
	Value  float64 `faker:"lat" json:"value"` // use lat as random float
	Active bool    `json:"active"`
	Time   int64   `faker:"unix_time" json:"time"`
}

// generates an instance of Payload with realistic random values
func generatePredictablePayload() Payload {
	var p Payload
	_ = faker.FakeData(&p)
	return p
}

// GenerateRandomJSON creates a JSON with predictable structure and random values
func GenerateRandomJSON() ([]byte, error) {
	return json.Marshal(generatePredictablePayload())
}

// GenerateRandomCBOR creates a CBOR with predictable structure and random values
func GenerateRandomCBOR() ([]byte, error) {
	return cbor.Marshal(generatePredictablePayload())
}

// GenerateSentence generates a random sentence for tests
func GenerateSentence() string {
	return faker.Sentence()
}

func GenerateSentimentPhrase() string {
	starts := []string{"I love", "I hate", "I think", "I feel", "I wish", "I see"}
	adjectives := []string{"great", "terrible", "amazing", "awful", "funny", "boring"}
	objects := []string{"this product", "the service", "the movie", "the food", "the weather", "the app"}
	return starts[rand.Intn(len(starts))] + " " + adjectives[rand.Intn(len(adjectives))] + " " + objects[rand.Intn(len(objects))] // #nosec G404 -- test data generator
}

func GenerateRandomDateTime() string {
	// Generate a random Unix timestamp between 1 and 10 years ago
	timestamp := rand.Int63n(10*365*24*3600) + (time.Now().Unix() - 10*365*24*3600) // #nosec G404 -- test data generator
	return time.Unix(timestamp, 0).Format(time.RFC3339Nano)
}

func GenerateNowDateTime() string {
	// Generate the current timestamp in RFC3339
	return time.Now().Format(time.RFC3339Nano)
}

var counter int = 0
var counterMutex = sync.Mutex{}

func GenerateCounter() int {
	counterMutex.Lock()
	defer counterMutex.Unlock()
	counter++
	return counter
}

func Interpolate(str string) ([]byte, error) {
	placeholders := map[string]TestPayloadType{
		"json":      TestPayloadJSON,
		"cbor":      TestPayloadCBOR,
		"sentiment": TestPayloadSentiment,
		"sentence":  TestPayloadSentence,
		"datetime":  TestPayloadDateTime,
		"nowtime":   TestPayloadNowTime,
		"counter":   TestPayloadCounter,
	}

	result := str
	for key, typ := range placeholders {
		ph := "{" + key + "}"

		if str == ph {
			// If the entire string is just the placeholder, return the generated value directly
			return typ.Generate()
		}

		if strings.Contains(result, ph) {
			val, err := typ.Generate()
			if err != nil {
				return nil, err
			}
			result = strings.ReplaceAll(result, ph, string(val))
		}
	}
	return []byte(result), nil
}

type TestPayloadType string

const (
	TestPayloadJSON      TestPayloadType = "json"
	TestPayloadCBOR      TestPayloadType = "cbor"
	TestPayloadSentiment TestPayloadType = "sentiment"
	TestPayloadSentence  TestPayloadType = "sentence"
	TestPayloadDateTime  TestPayloadType = "datetime" // to generate a timestamp
	TestPayloadNowTime   TestPayloadType = "nowtime"  // to generate the current timestamp
	TestPayloadCounter   TestPayloadType = "counter"  // to generate an incrementing counter (not implemented yet
)

func (t TestPayloadType) IsValid() bool {
	switch t {
	case TestPayloadJSON, TestPayloadCBOR, TestPayloadSentiment, TestPayloadSentence, TestPayloadDateTime, TestPayloadNowTime:
		return true
	}
	return false
}

func (t TestPayloadType) GetContentType() string {
	switch t {
	case TestPayloadJSON:
		return "application/json"
	case TestPayloadCBOR:
		return "application/cbor"
	case TestPayloadSentiment, TestPayloadSentence, TestPayloadDateTime, TestPayloadNowTime:
		return "text/plain"
	}
	return "application/octet-stream"
}

func (t TestPayloadType) Generate() ([]byte, error) {
	switch t {
	case TestPayloadJSON:
		return GenerateRandomJSON()
	case TestPayloadCBOR:
		return GenerateRandomCBOR()
	case TestPayloadSentiment:
		return []byte(GenerateSentimentPhrase()), nil
	case TestPayloadSentence:
		return []byte(GenerateSentence()), nil
	case TestPayloadDateTime:
		return []byte(GenerateRandomDateTime()), nil
	case TestPayloadNowTime:
		return []byte(GenerateNowDateTime()), nil
	case TestPayloadCounter:
		return []byte(fmt.Sprintf("%d", GenerateCounter())), nil
	}
	return nil, fmt.Errorf("unsupported test payload type: %s", t)
}
