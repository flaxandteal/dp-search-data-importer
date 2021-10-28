package handler

import (
	"context"
	"fmt"
	"os"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/log.go/v2/log"
)

//go:generate moq -out mock/result_writer.go -pkg mock . ResultWriter

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
	log.Info(ctx, "events handler called")

	//TODO : temp output - integrating with Elastic search is addressed in separate task
	f, err := os.Create("/tmp/dp-search-data-importer.txt")
	if err != nil {
		log.Error(ctx, "error while creating temp file", err)
	}
	log.Info(ctx, "temp file created")

	for i := 0; i < len(events); i++ {
		dataType := fmt.Sprintf("SearchDataImportModel.DataType, %s!", events[i].DataType)
		searchIndex := fmt.Sprintf("SearchDataImportModel.SearchIndex, %s!", events[i].SearchIndex)
		title := fmt.Sprintf("SearchDataImportModel.Title, %s!", events[i].Title)
		keywords := fmt.Sprintf("SearchDataImportModel.Keywords, %s!", events[i].Keywords)
		traceId := fmt.Sprintf("SearchDataImportModel.TraceId, %s!", events[i].TraceID)
		appendFile(ctx, dataType, searchIndex, keywords, title, traceId)
	}

	f.Close()
	log.Info(ctx, "event successfully handled")
	return nil
}

func appendFile(ctx context.Context, datatype string, searchIndex string, keywords string, title string, traceId string) {
	f, err := os.OpenFile("/tmp/dp-search-data-importer.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Info(ctx, "Error while opening a file", log.Data{"err": err})
		return
	}
	_, err = fmt.Fprintln(f, datatype, searchIndex, keywords, title, traceId)
	if err != nil {
		log.Info(ctx, "Error while appending the file", log.Data{"err": err})
	}
}
