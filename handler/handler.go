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

// BatchHandler handles batches of SearchDataImportModel events that contain CSV row data.
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
func (batchHandler BatchHandler) Handle(ctx context.Context, events []*models.SearchDataImportModel) error {

	logData := log.Data{
		"event": events,
	}
	log.Event(ctx, "events handler called", log.INFO, logData)

	publishedContents := make([]*models.SearchDataImportModel, 0, len(events))

	//TODO : temp output - Part 2 of task in another trello ticket -
	//https://trello.com/c/jhN0Yhlc/850-extend-search-data-importer-to-upsertupdate-add-docs-from-elasticsearch-ons-index-part-2
	f, err := os.Create("/tmp/dp-search-data-importer.txt")
	if err != nil {
		fmt.Println(err)
	}
	log.Event(ctx, "file created", log.INFO, log.Data{"publisheContents": f})
	log.Event(ctx, "slice size", log.INFO, log.Data{"publisheContents": publishedContents})

	for i := 0; i < len(publishedContents); i++ {
		dataType := fmt.Sprintf("publishedDataReceived.DataType, %s!", publishedContents[i].DataType)
		keywords := fmt.Sprintf("publishedDataReceived.Keywords, %s!", publishedContents[i].Keywords)
		appendFile(dataType, keywords)
	}

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
