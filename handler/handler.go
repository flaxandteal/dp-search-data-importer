package handler

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"

	dpelasticsearch "github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
)

const esDestIndex = "ons"

var _ event.Handler = (*BatchHandler)(nil)

var (
	syncWaitGroup sync.WaitGroup
	// TO-DO: The value should be configurable - constants / config value
	semaphore = make(chan int, 5)
)

// TO-DO: Move this model to the models package - should exist in dp-elasticsearch
type esBulkResponse struct {
	Took   int                  `json:"took"`
	Errors bool                 `json:"errors"`
	Items  []esBulkItemResponse `json:"items"`
}

type esBulkItemResponse map[string]esBulkItemResponseData

type esBulkItemResponseData struct {
	Index  string                  `json:"_index"`
	ID     string                  `json:"_id"`
	Status int                     `json:"status"`
	Error  esBulkItemResponseError `json:"error,omitempty"`
}

type esBulkItemResponseError struct {
	ErrorType string `json:"type"`
	Reason    string `json:"reason"`
	IndexUUID string `json:"index_uuid"`
	Shard     string `json:"shard"`
	Index     string `json:"index"`
}

// BatchHandler handles batches of SearchDataImportModel events that contain CSV row data.
type BatchHandler struct {
	esClient *dpelasticsearch.Client
}

// NewBatchHandler returns a BatchHandler.
func NewBatchHandler(esClient *dpelasticsearch.Client) *BatchHandler {

	return &BatchHandler{
		esClient: esClient,
	}
}

// Handle the given slice of SearchDataImport Model.
func (batchHandler BatchHandler) Handle(ctx context.Context, url string, events []*models.SearchDataImportModel) error {
	log.Info(ctx, "events handler called")

	//no events received. Nothing more to do.
	if len(events) == 0 {
		log.Info(ctx, "there are no events to handle")
		return nil
	}

	log.Info(ctx, "events received", log.Data{
		"no of events received": len(events),
	})

	// This will block if we've reached our concurrency limit (sem buffer size)
	err := batchHandler.sendToES(ctx, url, events)
	if err != nil {
		log.Fatal(ctx, "failed to send event to Elastic Search", err)
		return err
	}

	log.Info(ctx, "event successfully handled")
	return nil
}

// SendToES - Preparing the payload and sending to elastic search.
func (batchHandler BatchHandler) sendToES(ctx context.Context, esDestURL string, events []*models.SearchDataImportModel) error {

	// Wait on semaphore if we've reached our concurrency limit
	syncWaitGroup.Add(1)
	semaphore <- 1

	go func() {

		defer func() {
			<-semaphore
			syncWaitGroup.Done()
		}()

		log.Info(ctx, "go routine for inserting into ES starts")
		documentList := make(map[string]models.SearchDataImportModel)

		var bulkcreate []byte

		for _, event := range events {
			if event.Title == "" {
				log.Info(ctx, "No title for inbound event, no transformation possible")
				continue // break here
			}

			documentList[event.Title] = *event

			eventBulkRequestBody, err := prepareEventForBulkRequestBody(ctx, event, "create")
			if err != nil {
				log.Error(ctx, "unable to prepare bulk request body", err, log.Data{
					"event": *event,
				})
			}
			bulkcreate = append(bulkcreate, eventBulkRequestBody...)
		}

		jsonResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkcreate)
		if err != nil {
			log.Error(ctx, "error in response from elasticsearch for bulkcreate", err)
			return
		}

		var bulkRes esBulkResponse
		if err := json.Unmarshal(jsonResponse, &bulkRes); err != nil {
			log.Error(ctx, "error unmarshaling json", err)
		}

		if bulkRes.Errors {
			var bulkupdate []byte
			for _, r := range bulkRes.Items {
				if r["create"].Status == 409 {
					event := documentList[r["create"].ID]
					updateBulkBody, err := prepareEventForBulkRequestBody(ctx, &event, "update")
					if err != nil {
						log.Error(ctx, "error in update the bulk loader", err)
						return
					}
					bulkupdate = append(bulkupdate, updateBulkBody...)
				} else if r["create"].Status == 201 {
					continue
				} else {
					//logs for all failed transactions with different status than 409
					log.Error(ctx, "error inserting doc to ES", err,
						log.Data{
							"response status": r["create"].Status,
						})
					return
				}
			}
			_, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkupdate)
			if err != nil {
				log.Error(ctx, "error in response from elasticsearch for bulkupload while updating the event", err)
				return
			}
		}
	}()

	syncWaitGroup.Wait()
	log.Info(ctx, "go routine for inserting into ES ends")

	return nil
}

func prepareEventForBulkRequestBody(ctx context.Context, event *models.SearchDataImportModel, method string) (bulkbody []byte, err error) {
	t := transform.NewTransformer()
	esmodel := t.TransformEventModelToEsModel(event)

	if esmodel != nil {
		b, err := json.Marshal(esmodel)
		if err != nil {
			log.Fatal(ctx, "error marshal to json", err)
			return nil, err
		}

		bulkbody = append(bulkbody, []byte("{ \""+method+"\": { \"_id\": \""+esmodel.Title+"\" } }\n")...)
		bulkbody = append(bulkbody, b...)
		bulkbody = append(bulkbody, []byte("\n")...)
	}
	return bulkbody, nil
}
