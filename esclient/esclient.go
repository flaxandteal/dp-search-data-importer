package esclient

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	dpAwsauth "github.com/ONSdigital/dp-elasticsearch/v2/awsauth"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/v2/log"

	"github.com/pkg/errors"
)

const applicationJSON = "application/json"

// Client provides an interface with which to communicate with Elastic Search by way of HTTP requests
type Client interface {
	SubmitBulkToES(ctx context.Context, esDestIndex string, esDestURL string, bulk []byte) ([]byte, error)
}

// ClientImpl represents an instance of the elasticsearch client
type ClientImpl struct {
	awsSDKSigner *dpAwsauth.Signer
	client       dphttp.Clienter
	signRequests bool
	requester    Requester
}

// NewClient returns a concrete implementation of the Client interface
func NewClient(awsSDKSigner *dpAwsauth.Signer, client dphttp.Clienter, signRequests bool) Client {

	return &ClientImpl{
		awsSDKSigner: awsSDKSigner,
		client:       client,
		signRequests: signRequests,
		requester:    NewRequester(),
	}
}

// NewClientWithRequester returns a concrete implementation of the Client interface, taking a custom Requester
func NewClientWithRequester(awsSDKSigner *dpAwsauth.Signer, client dphttp.Clienter, signRequests bool, requester Requester) Client {

	return &ClientImpl{
		awsSDKSigner: awsSDKSigner,
		client:       client,
		signRequests: signRequests,
		requester:    requester,
	}
}

// SubmitBulkToES uses an HTTP post request to submit data to Elastic Search
func (cli *ClientImpl) SubmitBulkToES(
	ctx context.Context, esDestIndex string, esDestURL string, bulk []byte) ([]byte, error) {

	uri := fmt.Sprintf("%s/%s/_bulk", esDestURL, esDestIndex)
	bodyReader := bytes.NewReader(bulk)

	req, err := http.NewRequest("POST", uri, bodyReader)
	if err != nil {
		log.Error(ctx, "Error while getting new Request", err)
		return nil, err
	}

	if cli.signRequests {
		err := cli.awsSDKSigner.Sign(req, bodyReader, time.Now())
		if err != nil {
			logData := log.Data{"uri": uri, "index": esDestIndex}
			log.Event(ctx, "failed to sign request", log.ERROR, logData)
			return nil, err
		}
	}

	res, err := cli.requester.Post(bulk, uri)
	if err != nil {
		log.Info(ctx, "error posting request", log.Data{
			"bulk data": string(bulk),
			"err":       err})
		log.Error(ctx, "error posting request %s", err)
		return nil, err
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			log.Error(ctx, "failed to close response body after posting bulk to ES: %s", err)
		}
	}()

	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode > 299 {
		log.Info(ctx, "unexpected put response", log.Data{
			"Res Status": res.Status,
			"bulk data":  string(bulk),
		})
		log.Error(ctx, "unexpected put response", errors.New("invalid response"))
		return nil, errors.New("invalid response")
	}

	return b, err

}
