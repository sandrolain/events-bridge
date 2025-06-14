package testpayload

import (
	"encoding/json"
	"math/rand"

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
