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
	semaphore     = make(chan int, 5)
)

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

	// This will block if we've reached our concurrency limit (sem buffer size)
	err := batchHandler.sendToES(ctx, url, events)
	if err != nil {
		log.Fatal(ctx, "failed to send event to Elastic Search", err)
		return err
	}

	log.Info(ctx, "event successfully handled")
	return nil
}

// sendToES - Preparing the payload and sending bulk events to elastic search.
func (batchHandler BatchHandler) sendToES(ctx context.Context, esDestURL string, events []*models.SearchDataImportModel) error {

	// Wait on semaphore if we've reached our concurrency limit
	syncWaitGroup.Add(1)
	semaphore <- 1

	go func() {

		defer func() {
			<-semaphore
			syncWaitGroup.Done()
		}()

		log.Info(ctx, "go routine for inserting bulk events into ES starts")
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

		jsonCreateResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkcreate)
		if err != nil {
			log.Error(ctx, "error in response from elasticsearch for bulkcreate", err)
			return
		}

		var bulkCreateRes esBulkResponse
		if err := json.Unmarshal(jsonCreateResponse, &bulkCreateRes); err != nil {
			log.Error(ctx, "error unmarshaling json", err)
		}

		if bulkCreateRes.Errors {
			var bulkupdate []byte
			for _, resCreateItem := range bulkCreateRes.Items {
				if resCreateItem["create"].Status == 409 {
					event := documentList[resCreateItem["create"].ID]
					updateBulkBody, err := prepareEventForBulkRequestBody(ctx, &event, "update")
					if err != nil {
						log.Error(ctx, "error in update the bulk loader", err)
						return
					}
					bulkupdate = append(bulkupdate, updateBulkBody...)
				} else if resCreateItem["create"].Status == 201 {
					continue
				} else {
					log.Error(ctx, "error creating doc to ES", err,
						log.Data{
							"response status": resCreateItem["create"].Status,
							"response.title:": resCreateItem["create"].ID,
						})
					return
				}
			}

			jsonUpdateResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkupdate)
			if err != nil {
				log.Error(ctx, "error in response from elasticsearch for bulkupload while updating the event", err)
				return
			}

			var bulkUpdateRes esBulkResponse
			if err := json.Unmarshal(jsonUpdateResponse, &bulkUpdateRes); err != nil {
				log.Error(ctx, "error unmarshaling json", err)
			}

			if bulkUpdateRes.Errors {
				for _, resUpdateItem := range bulkUpdateRes.Items {
					if resUpdateItem["update"].Status == 200 {
						continue
					} else {
						log.Error(ctx, "error updating event to ES", err,
							log.Data{
								"response status": resUpdateItem["update"].Status,
								"response.title:": resUpdateItem["update"].ID,
							})
						return
					}
				}
			}
		}
	}()

	syncWaitGroup.Wait()
	log.Info(ctx, "go routine for inserting bulk events into ES ends")

	return nil
}

// prepareEventForBulkRequestBody - Preparing the payload to be inserted into the elastic search.
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
