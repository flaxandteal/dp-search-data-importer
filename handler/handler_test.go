package handler_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/handler/eventtest"
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
	Title:           "testTilte",
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
	Title:           "testTilte2",
	TraceID:         "testTraceID2",
}

var expectedEvents = []*models.SearchDataImportModel{
	expectedEvent1,
	expectedEvent2,
}

func TestPublishedContentExtractedHandler_Handle(t *testing.T) {

	Convey("Given a handler configured with a mock mapper", t, func() {
		mockResultWriter := &eventtest.ResultWriterMock{}

		batchHandler := handler.NewBatchHandler(mockResultWriter)
		filePath := "/tmp/dp-search-data-importer.txt"
		os.Remove(filePath)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(context.Background(), expectedEvents)

			Convey("The expected calls to the publish content mapper", func() {
				So(err, ShouldBeNil)
			})
			Convey("And the expected file is created", func() {
				if _, err := os.Stat(filePath); err != nil {
					if os.IsNotExist(err) {
						fmt.Printf("file does not exists")
						t.Fail()
					}
				}
			})
		})
	})
}
