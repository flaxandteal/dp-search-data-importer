package handler

import (
	"context"
	"fmt"
	"os"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out eventtest/result_writer.go -pkg eventtest . ResultWriter

var _ event.Handler = (*BatchHandler)(nil)

// BatchHandler handles batches of publishedContentModel events that contain CSV row data.
type BatchHandler struct {
	resultWriter ResultWriter
}

// TODO : ResultWriter dependency that outputs results
type ResultWriter interface{}

// NewBatchHandler returns a resultWriter TBD.
func NewBatchHandler(
	resultWriter ResultWriter) *BatchHandler {

	return &BatchHandler{
		resultWriter: resultWriter,
	}
}

// Handle the given slice of PublishedContent events.
func (batchHandler BatchHandler) Handle(ctx context.Context, events []*models.PublishedContentModel) error {

	logData := log.Data{
		"event": events,
	}
	log.Event(ctx, "events handler called", log.INFO, logData)

	publisheContents := make([]*models.PublishedContentModel, 0, len(events))

	//Print all incoming object and total size of slice
	//TODO : temp output
	f, err := os.Create("/tmp/dp-search-data-importer.txt")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("file created %v", f)

	// for _, publisheContent := range publisheContents {

	fmt.Printf("slice size %v", len(publisheContents))

	for i := 0; i < len(publisheContents); i++ {
		dataType := fmt.Sprintf("publishedDataReceived.DataType, %s!", publisheContents[i].DataType)
		keywords := fmt.Sprintf("publishedDataReceived.Keywords, %s!", publisheContents[i].Keywords)
		appendFile(dataType, keywords)
	}

	// }

	log.Event(ctx, "event successfully handled", log.INFO, logData)
	return nil
}

//TODO : temp output
func appendFile(datatype string, keywords string) {
	f, err := os.OpenFile("/tmp/dp-search-data-importer.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = fmt.Fprintln(f, datatype, keywords)
	if err != nil {
		fmt.Println(err)
	}
}
