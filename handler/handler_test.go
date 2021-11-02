package handler_test

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"
	. "github.com/smartystreets/goconvey/convey"
)

var expectedEvent1 = &models.SearchDataImportModel{
	DataType:        "testDataType",
	JobID:           "",
	SearchIndex:     "ONS",
	CDID:            "",
	DatasetID:       "",
	Keywords:        []string{"testkeyword1"},
	MetaDescription: "",
	Summary:         "",
	ReleaseDate:     "",
	Title:           "testTitle1",
	TraceID:         "testTraceID",
}

var expectedEvent2 = &models.SearchDataImportModel{
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

var expectedEvents = []*models.SearchDataImportModel{
	expectedEvent1,
	expectedEvent2,
}

func TestPublishedContentExtractedHandler_Handle(t *testing.T) {

	Convey("Given a handler configured with a mock mapper", t, func() {
		batchHandler := handler.NewBatchHandler()

		Convey("When handle is called", func() {
			err := batchHandler.Handle(context.Background(), expectedEvents)

			Convey("The expected calls to the publish content mapper", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
