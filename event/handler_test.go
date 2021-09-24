package event_test

import (
	"os"
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/event"
	. "github.com/smartystreets/goconvey/convey"
)


func TestPublishedContentExtractedHandler_Handle(t *testing.T) {

	Convey("Given a successful event handler, when Handle is triggered", t, func() {
		eventHandler := &event.PublishedContentHandler{}
		filePath := "/tmp/PublishedContentExtracted.txt"
		os.Remove(filePath)
		err := eventHandler.Handle(testCtx, &config.Config{OutputFilePath: filePath}, &testEvent)
		So(err, ShouldBeNil)
	})

	Convey("handler returns an error when cannot write to file", t, func() {
		eventHandler := &event.PublishedContentHandler{}
		filePath := ""
		err := eventHandler.Handle(testCtx, &config.Config{OutputFilePath: filePath}, &testEvent)
		So(err, ShouldNotBeNil)
	})
}
