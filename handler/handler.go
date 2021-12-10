package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"

	dpelasticsearch "github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
)

const esDestIndex = "ons"

var _ event.Handler = (*BatchHandler)(nil)

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

// Preparing the payload and sending bulk events to elastic search.
func (batchHandler BatchHandler) sendToES(ctx context.Context, esDestURL string, events []*models.SearchDataImportModel) error {

	log.Info(ctx, "inserting bulk events into ES starts")
	documentList := make(map[string]models.SearchDataImportModel)

	var bulkcreate []byte
	for _, event := range events {
		if event.Title == "" {
			log.Info(ctx, "No title for inbound event, no transformation possible")
			continue // break here
		}

		documentList[event.Title] = *event
		createBulkRequestBody, err := prepareEventForBulkRequestBody(ctx, event, "create")
		if err != nil {
			log.Error(ctx, "error in preparing the bulk for create", err, log.Data{
				"event": *event,
			})
		}
		bulkcreate = append(bulkcreate, createBulkRequestBody...)
	}

	fmt.Printf("++++++++++++bulkcreate \n%s", bulkcreate)

	jsonCreateResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkcreate)
	if err != nil {
		log.Error(ctx, "error in response from elasticsearch while creating the event", err)
		return err
	}

	var bulkCreateRes models.EsBulkResponse
	if err := json.Unmarshal(jsonCreateResponse, &bulkCreateRes); err != nil {
		log.Error(ctx, "error unmarshaling json", err)
	}

	if bulkCreateRes.Errors {
		var bulkupdate []byte
		for _, resCreateItem := range bulkCreateRes.Items {
			if resCreateItem["create"].Status == 409 {
				event := documentList[resCreateItem["create"].ID]
				updateBulkRequestBody, err := prepareEventForBulkRequestBody(ctx, &event, "update")
				if err != nil {
					log.Error(ctx, "error in preparing the bulk for update", err)
					continue
				}
				bulkupdate = append(bulkupdate, updateBulkRequestBody...)
			} else if resCreateItem["create"].Status == 201 {
				continue
			} else {
				log.Error(ctx, "error creating doc to ES", err,
					log.Data{
						"response status": resCreateItem["create"].Status,
						"response.title:": resCreateItem["create"].ID,
					})
				continue
			}
		}

		fmt.Printf("++++++++++++bulkupdate \n%s", bulkupdate)

		jsonUpdateResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkupdate)
		if err != nil {
			log.Error(ctx, "error in response from elasticsearch while updating the event", err)
		}

		var bulkUpdateRes models.EsBulkResponse
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
					continue
				}
			}
		}
	}

	log.Info(ctx, "inserting bulk events into ES ends")
	return nil
}

// Preparing the payload to be inserted into the elastic search.
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
