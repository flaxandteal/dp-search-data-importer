package handler_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/esclient/mock"
	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"

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

	testData = `{"cdid": "testCDID","summary": "testSummary","type": "testDataType"}`

	doFuncWithESResponse = func(ctx context.Context, cfg *config.Config, esDestIndex string, esDestURL string, bulk []byte) ([]byte, error) {
		testByteData := []byte(testData)
		return testByteData, nil
	}

	// Get Config
	testCfg, err = config.Get()
)

func TestDataImporterHandle(t *testing.T) {

	Convey("Given a handler configured with sucessful es updates", t, func() {

		esClientMock := &mock.ClientMock{
			SubmitBulkToESFunc: doFuncWithESResponse,
		}
		batchHandler := handler.NewBatchHandler(esClientMock)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, testCfg, testEvents)

			Convey("Then the bulk is inserted into elastic search", func() {
				So(esClientMock.SubmitBulkToESCalls(), ShouldNotBeEmpty)
				So(esClientMock.SubmitBulkToESCalls(), ShouldHaveLength, 1)
			})
			Convey("And the error is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
