package handler_test

import (
	"context"
	"os"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/handler/eventtest"
	"github.com/ONSdigital/dp-search-data-importer/models"
	. "github.com/smartystreets/goconvey/convey"
)

var expectedEvent1 = &models.PublishedContentModel{
	DataType:        "testDataType",
	JobID:           "",
	SearchIndex:     "ONS",
	CDID:            "",
	DatasetID:       "",
	Keywords:        []string{"testkeyword1"},
	MetaDescription: "",
	Summary:         "",
	ReleaseDate:     "",
	Title:           "",
	TraceID:         "testTraceID",
}

var expectedEvent2 = &models.PublishedContentModel{
	DataType:        "testDataType2",
	JobID:           "",
	SearchIndex:     "ONS",
	CDID:            "",
	DatasetID:       "",
	Keywords:        []string{"testkeyword2"},
	MetaDescription: "",
	Summary:         "",
	ReleaseDate:     "",
	Title:           "",
	TraceID:         "",
}

var expectedEvents = []*models.PublishedContentModel{
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
				// So(len(mockpublishedContentBatchMapper.MapCalls()), ShouldEqual, 1)
				// So(mockpublishedContentBatchMapper.MapCalls()[0].Row, ShouldEqual, expectedEvent.Row)
				// So(mockpublishedContentBatchMapper.MapCalls()[0].InstanceID, ShouldEqual, expectedEvent.InstanceID)
			})
			// Convey("And a file is created with all published content data"){

			// })
		})

	})
}
