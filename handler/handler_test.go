package handler_test

import (
	"context"
	"os"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"
	. "github.com/smartystreets/goconvey/convey"
)

var testEvent = models.PublishedContentExtracted{
	DataType:        "testDataType",
	JobID:           "",
	SearchIndex:     "ONS",
	CDID:            "",
	DatasetID:       "",
	Keywords:        "",
	MetaDescription: "",
	Summary:         "",
	ReleaseDate:     "",
	Title:           "",
	TraceID:         "testTraceID",
}

func TestPublishedContentExtractedHandler_Handle(t *testing.T) {

	Convey("Given a successful event handler, when Handle is triggered", t, func() {
		eventHandler := &handler.PublishedContentHandler{}
		filePath := "/tmp/PublishedContentExtracted.txt"
		os.Remove(filePath)
		err := eventHandler.Handle(context.Background(), &config.Config{OutputFilePath: filePath}, &testEvent)
		So(err, ShouldBeNil)
	})

	Convey("handler returns an error when cannot write to file", t, func() {
		eventHandler := &handler.PublishedContentHandler{}
		filePath := ""
		err := eventHandler.Handle(context.Background(), &config.Config{OutputFilePath: filePath}, &testEvent)
		So(err, ShouldNotBeNil)
	})
}
