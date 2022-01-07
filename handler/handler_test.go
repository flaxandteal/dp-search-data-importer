package handler_test

import (
	"bytes"
	"context"
	"errors"
	"io"
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

	esDestURL = "locahost:9999"

	expectedEvent1 = &models.SearchDataImportModel{
		UID:             "testTitle1",
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
		UID:             "testTitle1",
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

	setListOfPathsWithNoRetries = func(listOfPaths []string) {}

	mockSuccessESResponseWith409Error                  = "{\"took\":5,\"errors\":true,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle2\",\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\",\"reason\":\"[Help]: version conflict, document already exists (current version [1])\",\"index_uuid\":\"YNxkEkfcTp-SiMXOSqDvEA\",\"shard\":\"0\",\"index\":\"ons1637667136829001\"}}},{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle4\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
	mockSuccessESResponseWithNoError                   = "{\"took\":6,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle3\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
	mockSuccessESResponseWithBothCreateAndUpdateFailed = "{\"took\":5,\"errors\":true,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle2\",\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\",\"reason\":\"[Help]: version conflict, document already exists (current version [1])\",\"index_uuid\":\"YNxkEkfcTp-SiMXOSqDvEA\",\"shard\":\"0\",\"index\":\"ons1637667136829001\"}}},{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle4\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":400}}]}"
)

func successWithESResponseNoError() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWithNoError)),
		Header:     make(http.Header),
	}
}

func successWithESResponseError() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWith409Error)),
		Header:     make(http.Header),
	}
}

func failedWithESResponseError() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWithBothCreateAndUpdateFailed)),
		Header:     make(http.Header),
	}
}

func failedWithESUpdateResponseError() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWithBothCreateAndUpdateFailed)),
		Header:     make(http.Header),
	}
}

func failedWithESResponseInternalServerError() *http.Response {

	return &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(bytes.NewBufferString(`Internal Server Error`)),
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

func TestHandleWithTwoEventsBothEventCreated(t *testing.T) {

	var count int
	Convey("Given a handler configured with sucessful es updates for all two events is success", t, func() {

		doFuncWithValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			// Create bulk request succeeded with no failed resources
			// and Update bulk request not made
			return successWithESResponseNoError(), nil
		}
		httpCli := clientMock(doFuncWithValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("Then the error is nil and only create bulk is called but not update bulk request", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)
			})
		})
	})
}

func TestHandleWithTwoEventsWithOneEventCreateSuccessAndOtherUpdateSuccess(t *testing.T) {

	var count int
	Convey("Given a handler configured with sucessful es updates for two events with one create error", t, func() {

		doFuncWithValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			if count == 1 {
				// Create bulk request succeeded with one failed resources
				return successWithESResponseError(), nil
			} else {
				// Update bulk request succeeded with no failed resources
				return failedWithESUpdateResponseError(), nil
			}
		}
		httpCli := clientMock(doFuncWithValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("Then the error is nil and both create and update bulk request called", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 2)
			})
		})
	})
}

func TestHandleWithBothCreateAndUpdateFailedESResponse(t *testing.T) {

	var count int
	Convey("Given a handler configured with both events failed for es create and es updates", t, func() {

		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			if count == 1 {
				// Create bulk request succeeded with failed resources
				return failedWithESResponseError(), nil
			} else {
				// Update bulk request succeeded with failed resources
				return failedWithESUpdateResponseError(), nil
			}
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is nil and both create and update bulk request called", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 2)
			})
		})
	})
}

func TestHandleWithCreateAndInternalServerESResponse(t *testing.T) {

	var count int
	Convey("Given a handler configured with other failed es create request", t, func() {

		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			// Create bulk request failed
			// Update bulk request not made
			return failedWithESResponseInternalServerError(), nil
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is not nil and only create bulk request called", func() {
				So(err, ShouldResemble, errors.New("unexpected status code from api"))
				So(count, ShouldEqual, 1)
			})
		})
	})
}

func TestHandleWithCreateButUpdateWithInternalServerESResponse(t *testing.T) {

	var count int
	Convey("Given a handler configured with one success and other failed es updates", t, func() {

		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			if count == 1 {
				// Create bulk request succeeded with a failed resources
				return successWithESResponseError(), nil
			} else {
				// Update bulk request failed for internal server error
				return failedWithESResponseInternalServerError(), nil
			}
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is nil", func() {
				So(err, ShouldResemble, errors.New("unexpected status code from api"))
				So(count, ShouldEqual, 2)
			})
		})
	})
}
