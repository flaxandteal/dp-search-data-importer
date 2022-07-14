package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	dpMock "github.com/ONSdigital/dp-elasticsearch/v3/client/mocks"
	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"
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
	mockSuccessESResponseWithNoError = "{\"took\":6,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle3\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}"
)

func successWithESResponseNoError() []byte {
	res := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewBufferString(mockSuccessESResponseWithNoError)),
		Header:     make(http.Header),
	}
	resByte, _ := json.Marshal(res)
	return resByte
}

func TestHandleWithEventsCreated(t *testing.T) {

	Convey("Given a handler configured with successful es updates for all two events is success", t, func() {
		elasticSearchMock := &dpMock.ClientMock{
			BulkUpdateFunc: func(ctx context.Context, indexName string, url string, settings []byte) ([]byte, error) {
				return successWithESResponseNoError(), nil
			},
		}

		batchHandler := handler.NewBatchHandler(elasticSearchMock)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("Then the error is nil and it performed upsert action", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestHandleWithEventsUpdated(t *testing.T) {

	Convey("Given a handler configured with sucessful es updates for two events with one create error", t, func() {
		elasticSearchMock := &dpMock.ClientMock{
			BulkUpdateFunc: func(ctx context.Context, indexName string, url string, settings []byte) ([]byte, error) {
				return successWithESResponseNoError(), nil
			},
		}

		batchHandler := handler.NewBatchHandler(elasticSearchMock)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("Then the error is nil and it performed upsert action", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestHandleWithInternalServerESResponse(t *testing.T) {
	Convey("Given a handler configured with other failed es create request", t, func() {
		elasticSearchMock := &dpMock.ClientMock{
			BulkUpdateFunc: func(ctx context.Context, indexName string, url string, settings []byte) ([]byte, error) {
				return nil, errors.New("unexpected status code from api")
			},
		}
		batchHandler := handler.NewBatchHandler(elasticSearchMock)
		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, esDestURL, testEvents)

			Convey("And the error is not nil while performing upsert action", func() {
				So(err, ShouldResemble, errors.New("unexpected status code from api"))
			})
		})
	})
}
