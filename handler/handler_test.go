package handler_test

import (
	"context"
	"testing"

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
		Title:           "testTilte1",
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
		Title:           "testTilte2",
		TraceID:         "testTraceID2",
	}

	expectedEvent3 = &models.SearchDataImportModel{
		DataType:        "testDataType3",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword3"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "testTitle3",
		TraceID:         "testTraceID3",
	}

	expectedEvent4 = &models.SearchDataImportModel{
		DataType:        "testDataType4",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"testkeyword4"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "testTilte4",
		TraceID:         "testTraceID4",
	}

	testEvents = []*models.SearchDataImportModel{
		expectedEvent1,
		expectedEvent2,
		expectedEvent3,
		expectedEvent4,
	}
)

func TestDataImporterHandler_Handle(t *testing.T) {

	Convey("Given a handler configured with a mock mapper", t, func() {
		batchHandler := handler.NewBatchHandler()

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, testEvents)

			Convey("Then the error is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
