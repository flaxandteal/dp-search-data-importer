package esclient

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	dphttp "github.com/ONSdigital/dp-net/http"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx         context.Context
	url         = "http://localhost:999"
	expectedURL = "http://localhost:999/index/doctype/_search"

	mockClient = &dphttp.ClienterMock{
		DoFunc: doFuncWithValidResponse,
	}
	mockClientWithError = &dphttp.ClienterMock{
		DoFunc: doFuncWithError,
	}

	doFuncWithValidResponse = func(ctx context.Context, req *http.Request) (*http.Response, error) {
		return testResponse("testResponse"), nil
	}
	doFuncWithError = func(ctx context.Context, req *http.Request) (*http.Response, error) {
		return nil, errors.New("http error")
	}
)

func TestSearchWithoutAwsSdkSigner(t *testing.T) {

	Convey("Given that we want a request with a non-aws-sdk signer client for a response", t, func() {
		client := NewClient(nil, url, mockClient, false)

		Convey("When Search is called", func() {
			res, err := client.Search(ctx, "index", "doctype", []byte("search request"))

			Convey("Then a request with the search action should be posted", func() {
				So(err, ShouldBeNil)
				So(res, ShouldNotBeEmpty)
				So(mockClient.DoCalls(), ShouldHaveLength, 1)
				actualRequest := mockClient.DoCalls()[0].Req
				So(actualRequest.URL.String(), ShouldResemble, expectedURL)
				So(actualRequest.Method, ShouldResemble, "POST")
				body, err := ioutil.ReadAll(actualRequest.Body)
				So(err, ShouldBeNil)
				So(string(body), ShouldEqual, "search request")
			})
		})
	})
	Convey("Given that we want a request with a non-aws-sdk signer client for an error in response", t, func() {
		client := NewClient(nil, url, mockClientWithError, false)

		Convey("When Search is called", func() {
			res, err := client.Search(ctx, "index", "doctype", []byte("search request"))

			Convey("Then a request with the search action should be posted", func() {
				So(err, ShouldNotBeNil)
				So(res, ShouldBeNil)
				So(err.Error(), ShouldResemble, "http error")
				So(mockClientWithError.DoCalls(), ShouldHaveLength, 1)
				actualRequest := mockClientWithError.DoCalls()[0].Req
				So(actualRequest.URL.String(), ShouldResemble, expectedURL)
				So(actualRequest.Method, ShouldResemble, "POST")
				body, err := ioutil.ReadAll(actualRequest.Body)
				So(err, ShouldBeNil)
				So(string(body), ShouldEqual, "search request")
			})
		})
	})
}

func testResponse(body string) *http.Response {
	recorder := httptest.NewRecorder()
	recorder.WriteString(body)
	return recorder.Result()
}
