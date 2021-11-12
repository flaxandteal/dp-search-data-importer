package handler_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-elasticsearch/v2/awsauth"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"

	dpelasticSearch "github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
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

	testSigner *awsauth.Signer

	testCfg, _ = config.Get()

	emptyListOfPathsWithNoRetries = func() []string {
		return []string{}
	}

	setListOfPathsWithNoRetries = func(listOfPaths []string) {
		return
	}
)

func successESResponse() *http.Response {

	return &http.Response{
		StatusCode: 201,
		Body:       ioutil.NopCloser(bytes.NewBufferString(`Created`)),
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

		//ES Client initialisation : this needs to be mock
		doFuncWithInValidResponse := func(ctx context.Context, req *http.Request) (*http.Response, error) {
			return successESResponse(), nil
		}
		httpCli := clientMock(doFuncWithInValidResponse)
		esTestclient := dpelasticSearch.NewClientWithHTTPClientAndAwsSigner(esDestURL, testSigner, true, httpCli)

		batchHandler := handler.NewBatchHandler(esTestclient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, testCfg, testEvents)

			Convey("And the error is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
