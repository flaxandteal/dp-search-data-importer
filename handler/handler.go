package handler

import (
	"context"
	"fmt"
	"os"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/v2/log"
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

// Handle the given slice of SearchDataImport Model.
func (batchHandler BatchHandler) Handle(ctx context.Context, events []*models.SearchDataImportModel) error {

	logData := log.Data{
		"event": events,
	}
	log.Event(ctx, "events handler called", log.INFO, logData)

	//TODO : temp output - integrating with Elastic search is addressed in separate task
	f, err := os.Create("/tmp/dp-search-data-importer.txt")
	if err != nil {
		fmt.Println(err)
	}
	log.Event(ctx, "file created", log.INFO, log.Data{"SearchDataImportModel": f})

	for i := 0; i < len(events); i++ {
		dataType := fmt.Sprintf("SearchDataImportModel.DataType, %s!", events[i].DataType)
		searchIndex := fmt.Sprintf("SearchDataImportModel.SearchIndex, %s!", events[i].SearchIndex)
		title := fmt.Sprintf("SearchDataImportModel.Title, %s!", events[i].Title)
		keywords := fmt.Sprintf("SearchDataImportModel.Keywords, %s!", events[i].Keywords)
		traceId := fmt.Sprintf("SearchDataImportModel.TraceId, %s!", events[i].TraceID)
		appendFile(dataType, searchIndex, keywords, title, traceId)
	}

	log.Event(ctx, "event successfully handled", log.INFO, logData)
	return nil
}

func appendFile(datatype string, searchIndex string, keywords string, title string, traceId string) {
	f, err := os.OpenFile("/tmp/dp-search-data-importer.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = fmt.Fprintln(f, datatype, searchIndex, keywords, title, traceId)
	if err != nil {
		fmt.Println(err)
	}
}
