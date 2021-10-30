package esclient

import (
	"bytes"
	"net/http"
)

// Requester provides an interface by which to execute HTTP requests
type Requester interface {
	Post(bulk []byte, uri string) (*http.Response, error)
}

// Request provides a concrete implementation of the Requester interface
type Request struct{}

// NewRequester returns a concrete implementation of the Requester interface
func NewRequester() Requester {

	return &Request{}
}

// Post performs a POST request, using a provided body, against a given uri
func (cli *Request) Post(body []byte, uri string) (*http.Response, error) {

	return http.Post(uri, applicationJSON, bytes.NewReader(body))
}
