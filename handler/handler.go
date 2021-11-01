package handler

import (
	"context"
	"fmt"

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
	log.Info(ctx, "events handler called")

	//TODO : temp output - integrating with Elastic search is addressed in separate task
	for i := 0; i < len(events); i++ {
		dataType := fmt.Sprintf("SearchDataImportModel.DataType, %s!", events[i].DataType)
		searchIndex := fmt.Sprintf("SearchDataImportModel.SearchIndex, %s!", events[i].SearchIndex)
		title := fmt.Sprintf("SearchDataImportModel.Title, %s!", events[i].Title)
		keywords := fmt.Sprintf("SearchDataImportModel.Keywords, %s!", events[i].Keywords)
		traceId := fmt.Sprintf("SearchDataImportModel.TraceId, %s!", events[i].TraceID)

		logData := log.Data{
			"dataType":    dataType,
			"searchIndex": searchIndex,
			"title":       title,
			"keywords":    keywords,
			"traceId":     traceId,
		}
		log.Info(ctx, "Event in Pipeline to be processed for Elastic Search", log.INFO, logData)
	}

	log.Info(ctx, "event successfully handled")
	return nil
}
