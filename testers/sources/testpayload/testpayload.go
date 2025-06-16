package testpayload

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-faker/faker/v4"
)

func init() {
}

// Payload rappresenta la struttura prevedibile del payload
// faker annota i campi per la generazione automatica
// https://github.com/go-faker/faker#supported-tags
type Payload struct {
	ID     string  `faker:"uuid_hyphenated" json:"id"`
	Name   string  `faker:"name" json:"name"`
	Value  float64 `faker:"lat" json:"value"` // usa lat come float random
	Active bool    `json:"active"`
	Time   int64   `faker:"unix_time" json:"time"`
}

// genera un'istanza di Payload con valori random realistici
func generatePredictablePayload() Payload {
	var p Payload
	_ = faker.FakeData(&p)
	return p
}

// GenerateRandomJSON genera una mappa JSON con struttura prevedibile e valori random
func GenerateRandomJSON() ([]byte, error) {
	return json.Marshal(generatePredictablePayload())
}

// GenerateRandomCBOR genera una mappa CBOR con struttura prevedibile e valori random
func GenerateRandomCBOR() ([]byte, error) {
	return cbor.Marshal(generatePredictablePayload())
}

// GenerateRandomPhrase genera una frase random per sentiment analysis
func GenerateSentence() string {
	return faker.Sentence()
}

func GenerateSentimentPhrase() string {
	starts := []string{"I love", "I hate", "I think", "I feel", "I wish", "I see"}
	adjectives := []string{"great", "terrible", "amazing", "awful", "funny", "boring"}
	objects := []string{"this product", "the service", "the movie", "the food", "the weather", "the app"}
	return starts[rand.Intn(len(starts))] + " " + adjectives[rand.Intn(len(adjectives))] + " " + objects[rand.Intn(len(objects))]
}

func GenerateRandomTime() string {
	// Genera un timestamp Unix random tra 1 e 10 anni fa
	timestamp := rand.Int63n(10*365*24*3600) + (time.Now().Unix() - 10*365*24*3600)
	return time.Unix(timestamp, 0).Format(time.RFC3339Nano)
}

func GenerateNowTime() string {
	// Genera il timestamp corrente in RFC3339
	return time.Now().Format(time.RFC3339Nano)
}

func Generate(typ TestPayloadType) ([]byte, error) {
	switch typ {
	case TestPayloadJSON:
		return GenerateRandomJSON()
	case TestPayloadCBOR:
		return GenerateRandomCBOR()
	case TestPayloadSentiment:
		return []byte(GenerateSentimentPhrase()), nil
	case TestPayloadSentence:
		return []byte(GenerateSentence()), nil
	case TestPayloadDateTime:
		return []byte(GenerateRandomTime()), nil
	case TestPayloadNowTime:
		return []byte(GenerateNowTime()), nil
	}
	return nil, fmt.Errorf("unsupported test payload type: %s", typ)
}

func Interpolate(str string) ([]byte, error) {
	placeholders := map[string]TestPayloadType{
		"json":      TestPayloadJSON,
		"cbor":      TestPayloadCBOR,
		"sentiment": TestPayloadSentiment,
		"sentence":  TestPayloadSentence,
		"datetime":  TestPayloadDateTime,
		"nowtime":   TestPayloadNowTime,
	}

	result := str
	for key, typ := range placeholders {
		ph := "{" + key + "}"
		if strings.Contains(result, ph) {
			val, err := Generate(typ)
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
	TestPayloadDateTime  TestPayloadType = "datetime" // per generare timestamp
	TestPayloadNowTime   TestPayloadType = "nowtime"  // per generare timestamp corrente
)

func (t TestPayloadType) IsValid() bool {
	switch t {
	case TestPayloadJSON, TestPayloadCBOR, TestPayloadSentiment, TestPayloadSentence, TestPayloadDateTime, TestPayloadNowTime:
		return true
	}
	return false
}
