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

const (
	esDestIndex = "ons"
	create      = "create"
)

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

	var bulkupsert []byte
	for _, event := range events {
		if event.UID == "" {
			log.Info(ctx, "No uid for inbound kafka event, no transformation possible")
			continue // break here
		}

		upsertBulkRequestBody, err := prepareEventForBulkUpsertRequestBody(ctx, event)
		if err != nil {
			log.Error(ctx, "error in preparing the bulk for upsert", err, log.Data{
				"event": *event,
			})
			continue
		}
		bulkupsert = append(bulkupsert, upsertBulkRequestBody...)
	}

	jsonUpsertResponse, _, err := batchHandler.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkupsert)
	if err != nil {
		if jsonUpsertResponse == nil {
			log.Error(ctx, "server error while upserting the event", err)
			return err
		}
		log.Warn(ctx, "error in response from elasticsearch while upserting the event", log.FormatErrors([]error{err}))
	}

	var bulkRes models.EsBulkResponse
	if err := json.Unmarshal(jsonUpsertResponse, &bulkRes); err != nil {
		log.Error(ctx, "error unmarshaling json", err)
		return err
	}
	if bulkRes.Errors {
		for _, resUpsertItem := range bulkRes.Items {
			if resUpsertItem[create].Status == 409 {
				continue
			} else {
				log.Error(ctx, "error upserting doc to ES", err,
					log.Data{
						"response.uid:":   resUpsertItem[create].ID,
						"response status": resUpsertItem[create].Status,
					})
				target--
				continue
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
func prepareEventForBulkUpsertRequestBody(ctx context.Context, sdModel *models.SearchDataImportModel) (bulkbody []byte, err error) {

	uid := sdModel.UID
	t := transform.NewTransformer()
	esModel := t.TransformEventModelToEsModel(sdModel)

	if esModel != nil {
		b, err := json.Marshal(esModel)
		if err != nil {
			log.Error(ctx, "error marshal to json while preparing bulk request", err)
			return nil, err
		}

		log.Info(ctx, "uid while preparing bulk request for upsert", log.Data{"uid": uid})
		bulkbody = append(bulkbody, []byte("{ \""+"update"+"\": { \"_id\": \""+uid+"\" } }\n")...)
		bulkbody = append(bulkbody, []byte("{")...)
		bulkbody = append(bulkbody, []byte("\"doc\":")...)
		bulkbody = append(bulkbody, b...)
		bulkbody = append(bulkbody, []byte(",\"doc_as_upsert\": true")...)
		bulkbody = append(bulkbody, []byte("}")...)
		bulkbody = append(bulkbody, []byte("\n")...)
	}
	return bulkbody, nil
}
