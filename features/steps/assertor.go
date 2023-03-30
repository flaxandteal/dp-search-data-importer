package steps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

// ElasticsearchCall represents a call to Elasticsearch, containing
// the http request pointer
// and the array of queries obtained from the request body
type ElasticsearchCall struct {
	Req     *http.Request
	Queries []string
}

// ElasticsearchAssertor implements httpfake.Assertor so it can be registered to component test HTTP mocks
// It contains an array of expected queries and a channel where the ElasticsearchCalls will be sent
type ElasticsearchAssertor struct {
	expected []string
	called   chan *ElasticsearchCall
}

// NewAssertor creates a new Elasticsearch Assertor
// If a non-nil expected byte array is provided, it will be converted into a slice of pretty json strings
func NewAssertor(expected []byte) (*ElasticsearchAssertor, error) {
	var (
		err            error
		expectedString = []string{}
	)

	if expected != nil {
		expectedString, err = multiJSONPretty(expected)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal response body: %w", err)
		}
	}

	return &ElasticsearchAssertor{
		expected: expectedString,
		called:   make(chan *ElasticsearchCall),
	}, nil
}

// Expected returns a slice of expected queries
func (esa *ElasticsearchAssertor) Expected() []string {
	return esa.expected
}

// Assert converts the request body to a slice of pretty json strings
// and sends them to the 'called' channel, so the caller can do the validation
func (esa *ElasticsearchAssertor) Assert(r *http.Request) error {
	var bodyStrings []string
	defer func() {
		esa.called <- &ElasticsearchCall{
			Req:     r,
			Queries: bodyStrings,
		}
	}()

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}

	bodyStrings, err = multiJSONPretty(b)
	if err != nil {
		return fmt.Errorf("failed to marshal response body: %w", err)
	}

	return nil
}

func jsonPretty(data []byte) (string, error) {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, data, "", "  "); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func multiJSONPretty(data []byte) ([]string, error) {
	ret := []string{} // return slice
	depth := 0        // json tree depth level
	startIndex := 0   // current block first index

	for i, b := range string(data) {
		if b == '{' {
			if depth == 0 {
				startIndex = i // new block start identified
			}
			depth += 1
		}

		endOfBlock := false
		if b == '}' {
			depth -= 1
			if depth == 0 {
				endOfBlock = true // end of current block identified
			}
		}

		if endOfBlock {
			re, err := jsonPretty(data[startIndex : i+1])
			if err != nil {
				return nil, err
			}
			ret = append(ret, re)
		}
	}

	return ret, nil
}

func (esa *ElasticsearchAssertor) Log(t testing.TB) {}

func (esa *ElasticsearchAssertor) Error(t testing.TB, err error) {
	t.Error(err)
}
