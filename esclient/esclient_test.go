package esclient

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"

	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/dp-search-data-importer/config"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx context.Context

	testHttpClient = &dphttp.ClienterMock{
		DoFunc: doFuncWithValidResponse,
	}

	doFuncWithValidResponse = func(ctx context.Context, req *http.Request) (*http.Response, error) {
		return successESResponse(), nil
	}
)

func TestUnitSubmitBulkToES(t *testing.T) {

	ctrl := gomock.NewController(t)

	mr := NewMockRequester(ctrl)
	mc := NewClientWithRequester(testHttpClient, mr)

	bulk := make([]byte, 1)
	esDestURL := "esDestURL"
	esIndex := "esIndex"
	esDestType := "docType"
	esDestIndex := fmt.Sprintf("%s/%s", esIndex, esDestType)

	uri := esDestURL + "/" + esDestIndex + "/_bulk"

	cfg, err := config.Get()
	if err != nil {
		t.Fatal(ctx, "error getting config", err)
		t.Fail()
	}

	Convey("Given a successful post of bulks to Elastic Search", t, func() {
		mr.EXPECT().Post(bulk, uri).Return(successESResponse(), nil)

		Convey("When SubmitBulkToES is called", func() {
			returnedBytes, err := mc.SubmitBulkToES(ctx, cfg, esDestIndex, esDestURL, bulk)

			Convey("Then returnedBytes should not be nil", func() {
				So(returnedBytes, ShouldNotBeNil)
			})
			Convey("And err should be nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given an unsuccessful post of bulks to Elastic Search", t, func() {
		mr.EXPECT().Post(bulk, uri).Return(unsuccessfulESResponse(), errors.New("error posting bulk"))

		Convey("When SubmitBulkToES is called", func() {
			returnedBytes, err := mc.SubmitBulkToES(ctx, cfg, esDestIndex, esDestURL, bulk)

			Convey("Then returnedBytes should be nil", func() {
				So(returnedBytes, ShouldBeNil)
			})
			Convey("And err should not be nil", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})

	Convey("Given an unexpected response when posting bulks to Elastic Search", t, func() {
		mr.EXPECT().Post(bulk, uri).Return(unsuccessfulESResponse(), nil)

		Convey("When SubmitBulkToES is called", func() {
			returnedBytes, err := mc.SubmitBulkToES(ctx, cfg, esDestIndex, esDestURL, bulk)

			Convey("Then returnedBytes should be nil", func() {
				So(returnedBytes, ShouldBeNil)
			})
			Convey("And err should not be nil", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func successESResponse() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       ioutil.NopCloser(bytes.NewBufferString(`Created`)),
		Header:     make(http.Header),
	}
}

func unsuccessfulESResponse() *http.Response {

	return &http.Response{
		StatusCode: 500,
		Body:       ioutil.NopCloser(bytes.NewBufferString(`Internal server error`)),
		Header:     make(http.Header),
	}
}
