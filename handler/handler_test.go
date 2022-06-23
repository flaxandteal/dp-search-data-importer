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
	esDestURL   = "locahost:9999"

	expectedEvent1 = &models.SearchDataImportModel{
		UID:             "uid1",
		DataType:        "anyDataType1",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"anykeyword1"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "anyTitle1",
		TraceID:         "anyTraceID1",
		DateChanges:     []models.ReleaseDateDetails{},
		Cancelled:       false,
		Finalised:       false,
		ProvisionalDate: "",
		Published:       false,
		Survey:          "",
		Language:        "",
		CanonicalTopic:  "",
	}

	expectedEvent2 = &models.SearchDataImportModel{
		UID:             "uid2",
		DataType:        "anyDataType2",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"anykeyword2"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "anyTitle2",
		TraceID:         "anyTraceID2",
	}

	testEvents = []*models.SearchDataImportModel{
		expectedEvent1,
		expectedEvent2,
	}

	emptyListOfPathsWithNoRetries = func() []string {
		return []string{}
	}

	setListOfPathsWithNoRetries = func(listOfPaths []string) {}

	mockSuccessESResponseWith409Error = "{\"took\":5,\"errors\":true,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle2\",\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\",\"reason\":\"[Help]: version conflict, document already exists (current version [1])\",\"index_uuid\":\"YNxkEkfcTp-SiMXOSqDvEA\",\"shard\":\"0\",\"index\":\"ons1637667136829001\"}}},{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle4\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
	mockSuccessESResponseWithNoError  = "{\"took\":6,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle3\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
)

func successWithESResponseNoError() *http.Response {

	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWithNoError)),
		Header:     make(http.Header),
	}
}

func successWithESResponseError() *http.Response {

	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWith409Error)),
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

func TestHandleWithEventsCreated(t *testing.T) {

	Convey("Given a handler configured with successful es updates for all two events is success", t, func() {
		count := 0

		doFuncWithValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			return successWithESResponseNoError(), nil
		}
		httpCli := clientMock(doFuncWithValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("Then the error is nil and it performed upsert action", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)
			})
		})
	})
}

func TestHandleWithEventsUpdated(t *testing.T) {

	Convey("Given a handler configured with sucessful es updates for two events with one create error", t, func() {

		count := 0
		doFuncWithValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			return successWithESResponseError(), nil
		}
		httpCli := clientMock(doFuncWithValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("Then the error is nil and it performed upsert action", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)
			})
		})
	})
}

func TestHandleWithInternalServerESResponse(t *testing.T) {

	Convey("Given a handler configured with other failed es create request", t, func() {
		count := 0

		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			count++
			return failedWithESResponseInternalServerError(), nil
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticsearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, nil, false, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is not nil while performing upsert action", func() {
				So(err, ShouldResemble, errors.New("unexpected status code from api"))
				So(count, ShouldEqual, 1)
			})
		})
	})
}
