package esclient

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"time"

	esauth "github.com/ONSdigital/dp-elasticsearch/v2/awsauth"
	dphttp "github.com/ONSdigital/dp-net/http"

	"strings"

	"github.com/pkg/errors"
)

// ClientImpl provides a concrete implementation of the Client interface
// Client represents an instance of the elasticsearch client
type ClientImpl struct {
	awsRegion    string
	awsSDKSigner *esauth.Signer
	awsService   string
	url          string
	client       dphttp.Clienter
	signRequests bool
}

// NewClient returns a concrete implementation of the Client interface
func NewClient(url string,
	client dphttp.Clienter,
	signRequests bool,
	awsSDKSigner *esauth.Signer,
	awsService string,
	awsRegion string) *ClientImpl {
	return &ClientImpl{
		awsSDKSigner: awsSDKSigner,
		awsRegion:    awsRegion,
		awsService:   awsService,
		url:          strings.TrimRight(url, "/"),
		client:       client,
		signRequests: signRequests,
	}
}

// Search is a method that wraps the Search function of the elasticsearch package
func (cli *ClientImpl) Search(ctx context.Context, index string, docType string, request []byte) ([]byte, error) {
	return cli.post(ctx, index, docType, "_search", request)
}

func (cli *ClientImpl) post(ctx context.Context, index string, docType string, action string, request []byte) ([]byte, error) {
	reader := bytes.NewReader(request)
	req, err := http.NewRequest("POST", cli.url+"/"+buildContext(index, docType)+action, reader)
	if err != nil {
		return nil, err
	}

	if cli.signRequests {
		if err = cli.awsSDKSigner.Sign(req, reader, time.Now()); err != nil {
			return nil, err
		}
	}

	resp, err := cli.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "elaticsearchClient error reading post response body")
	}

	return response, nil
}

func buildContext(index string, docType string) string {
	context := ""
	if len(index) > 0 {
		context = index + "/"
		if len(docType) > 0 {
			context += docType + "/"
		}
	}
	return context
}
