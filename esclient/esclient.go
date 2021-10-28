package esclient

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-elasticsearch/v2/awsauth"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/v2/log"

	"strings"

	"github.com/pkg/errors"
)

// ClientImpl represents an instance of the elasticsearch client
type ClientImpl struct {
	awsSDKSigner *awsauth.Signer
	url          string
	client       dphttp.Clienter
	signRequests bool
}

// NewClient returns a concrete implementation of the Client interface
func NewClient(
	awsSDKSigner *awsauth.Signer,
	url string,
	client dphttp.Clienter,
	signRequests bool,
) *ClientImpl {
	return &ClientImpl{
		awsSDKSigner: awsSDKSigner,
		url:          strings.TrimRight(url, "/"),
		client:       client,
		signRequests: signRequests,
	}
}

// Search is a method that wraps the Search function of the elasticsearch package
func (cli *ClientImpl) Search(ctx context.Context, index string, docType string, payload []byte) ([]byte, error) {
	return cli.post(ctx, index, docType, "_search", payload)
}

func (cli *ClientImpl) post(ctx context.Context, index string, docType string, action string, payload []byte) ([]byte, error) {
	bodyReader := bytes.NewReader(payload)
	url := cli.url + "/" + buildContext(index, docType) + action
	req, err := http.NewRequest("POST", url, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-type", "application/json")
	req.Header.Add("Authorization", "testAuthorization")

	if cli.signRequests {
		if err = cli.awsSDKSigner.Sign(req, bodyReader, time.Now()); err != nil {
			logData := log.Data{"url": url, "index": index}
			log.Event(ctx, "failed to sign request", log.ERROR, logData)
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
