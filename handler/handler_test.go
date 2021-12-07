package handler_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"

	dpelasticsearch "github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
	dphttp "github.com/ONSdigital/dp-net/http"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	testContext = context.Background()

	expectedEvent1 = &models.SearchDataImportModel{
		DataType:        "testDataType1",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword1"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "testTitle1",
		TraceID:         "testTraceID1",
	}

	expectedEvent2 = &models.SearchDataImportModel{
		DataType:        "testDataType2",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword2"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "testTitle2",
		TraceID:         "testTraceID2",
	}

	testEvents = []*models.SearchDataImportModel{
		expectedEvent1,
		expectedEvent2,
	}

	emptyListOfPathsWithNoRetries = func() []string {
		return []string{}
	}

	setListOfPathsWithNoRetries = func(listOfPaths []string) {
		return
	}

	mockSuccessESResponseWith409Error = "{\"took\":5,\"errors\":true,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle2\",\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\",\"reason\":\"[Help]: version conflict, document already exists (current version [1])\",\"index_uuid\":\"YNxkEkfcTp-SiMXOSqDvEA\",\"shard\":\"0\",\"index\":\"ons1637667136829001\"}}},{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testType5\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
	mockSuccessESResponseWithNoError  = "{\"took\":6,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"Monthly gross domestic product: time series\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
)

func successWithESResponseNoError() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       ioutil.NopCloser(bytes.NewBufferString(mockSuccessESResponseWithNoError)),
		Header:     make(http.Header),
	}
}

func successWithESResponseError() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       ioutil.NopCloser(bytes.NewBufferString(mockSuccessESResponseWith409Error)),
		Header:     make(http.Header),
	}
}

func clientMock(doFunc func(ctx context.Context, request *http.Request) (*http.Response, error)) *dphttp.ClienterMock {
	return &dphttp.ClienterMock{
		DoFunc:                    doFunc,
		GetPathsWithNoRetriesFunc: emptyListOfPathsWithNoRetries,
		SetPathsWithNoRetriesFunc: setListOfPathsWithNoRetries,
	}
}

func TestDataImporterHandle(t *testing.T) {

	Convey("Given a handler configured with sucessful es updates", t, func() {
		esDestURL := "locahost:9999"

		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			return successWithESResponseNoError(), nil
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestDataImporterHandleWithFailedESResponse(t *testing.T) {

	Convey("Given a handler configured with one success and other failed es updates", t, func() {
		esDestURL := "locahost:9999"

		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			return successWithESResponseError(), nil
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
