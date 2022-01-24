package handler

import (
	"context"
	"encoding/json"

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

	// no events received. Nothing more to do.
	if len(events) == 0 {
		log.Info(ctx, "there are no events to handle")
		return nil
	}

	err := batchHandler.sendToES(ctx, url, events)
	if err != nil {
		log.Error(ctx, "failed to send event to Elastic Search", err)
		return err
	}

	log.Info(ctx, "event successfully handled")
	return nil
}

// Preparing the payload and sending bulk events to elastic search.
func (batchHandler BatchHandler) sendToES(ctx context.Context, esDestURL string, events []*models.SearchDataImportModel) error {

	log.Info(ctx, "bulk events into ES starts")
	target := len(events)
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
			continue
		}
		bulkcreate = append(bulkcreate, createBulkRequestBody...)
	}

	jsonCreateResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkcreate)
	if err != nil {
		if jsonCreateResponse == nil {
			return err
		}
		log.Warn(ctx, "error in response from elasticsearch while creating the event", log.FormatErrors([]error{err}))
	}

	var bulkCreateRes models.EsBulkResponse
	err = json.Unmarshal(jsonCreateResponse, &bulkCreateRes)
	if err != nil {
		log.Error(ctx, "error unmarshaling jsonCreateResponse", err)
		return err
	}

	if bulkCreateRes.Errors {
		var bulkupdate []byte
		for _, resCreateItem := range bulkCreateRes.Items {
			if resCreateItem["create"].Status == 409 {
				esEvent := documentList[resCreateItem["create"].ID]
				updateBulkRequestBody, err := prepareEventForBulkRequestBody(ctx, &esEvent, "update")
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

		jsonUpdateResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkupdate)
		if err != nil {
			log.Error(ctx, "error in response from elasticsearch while updating the event", err)
			return err
		}

		var bulkUpdateRes models.EsBulkResponse
		err = json.Unmarshal(jsonUpdateResponse, &bulkUpdateRes)
		if err != nil {
			log.Error(ctx, "error unmarshaling bulkUpdateRes", err)
			return err
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
					target--
					continue
				}
			}
		}
	}
	log.Info(ctx, "documents bulk uploaded to elasticsearch", log.Data{
		"documents_received": len(events),
		"documents_inserted": target,
	})
	return nil
}

// Preparing the payload to be inserted into the elastic search.
func prepareEventForBulkRequestBody(ctx context.Context, sdModel *models.SearchDataImportModel, method string) (bulkbody []byte, err error) {
	t := transform.NewTransformer()
	esModel := t.TransformEventModelToEsModel(sdModel)

	if esModel != nil {
		b, err := json.Marshal(esModel)
		if err != nil {
			log.Error(ctx, "error marshal to json while preparing bulk request", err)
			return nil, err
		}

		uid := esModel.UID
		log.Info(ctx, "UID while preparing bulk request", log.Data{"UID": uid})
		bulkbody = append(bulkbody, []byte("{ \""+method+"\": { \"_id\": \""+uid+"\" } }\n")...)
		bulkbody = append(bulkbody, b...)
		bulkbody = append(bulkbody, []byte("\n")...)
	}
	return bulkbody, nil
}
