package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ONSdigital/dp-api-clients-go/v2/nlp/berlin"
	dpelasticsearch "github.com/ONSdigital/dp-elasticsearch/v3/client"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
)

const (
	esDestIndex  = "ons"
	esRespCreate = "create"
)

// BatchHandler handles batches of SearchDataImportModel events that contain CSV row data.
type BatchHandler struct {
	berlinClient berlin.Clienter
	esClient     dpelasticsearch.Client
	esURL        string
}

// NewBatchHandler returns a BatchHandler.
func NewBatchHandler(esClient dpelasticsearch.Client, cfg *config.Config, brlClient berlin.Clienter) *BatchHandler {
	return &BatchHandler{
		berlinClient: brlClient,
		esClient:     esClient,
		esURL:        cfg.ElasticSearchAPIURL,
	}
}

func (h *BatchHandler) Handle(ctx context.Context, batch []kafka.Message) error {
	// no events received. Nothing more to do (this scenario should not happen)
	fmt.Println("enters")
	if len(batch) == 0 {
		log.Info(ctx, "there are no events to handle")
		return nil
	}

	fmt.Println("###############################################", len(batch))
	// unmarshal all events in batch
	events := make([]*models.SearchDataImport, len(batch))

	// unmarshal all events in batch and sort them by indexes
	eventMap := make(map[string][]*models.SearchDataImport)
	for _, msg := range batch {
		e := &models.SearchDataImport{}
		s := schema.SearchDataImportEvent

		fmt.Println("get msg data from batch")
		fmt.Println("get msg data from batch")
		fmt.Println(string(msg.GetData()))
		fmt.Println(string(msg.GetData()))
		if err := s.Unmarshal(msg.GetData(), e); err != nil {
			return &Error{
				err: fmt.Errorf("failed to unmarshal event: %w", err),
				logData: map[string]interface{}{
					"msg_data": string(msg.GetData()),
				},
			}
		}

		brlOpt := berlin.OptInit()
		brlOpt.Q(e.Summary)

		brlResults, err := h.berlinClient.GetBerlin(ctx, brlOpt)
		if err != nil {
			log.Error(ctx, "There was an error getting berlin results", err)
		}

		fmt.Println(brlResults)
		// example how to fill the location
		e.Location = brlResults.Matches[0].Loc.Subdivision[0]
		fmt.Println("keywords", e.Location)
		fmt.Println("keywords", e)

		eventMap[e.SearchIndex] = append(eventMap[e.SearchIndex], e)
	}

	log.Info(ctx, "batch of events received", log.Data{"len": len(events)})
	fmt.Println("event looks like: ", eventMap)
	for _, eventsByIdx := range eventMap {
		// send batch to elasticsearch concurrently
		go func(events []*models.SearchDataImport) {
			// send batch to Elasticsearch
			err := h.sendToES(ctx, events)
			if err != nil {
				log.Error(ctx, "failed to send events to Elastic Search", err)
			}
		}(eventsByIdx)
	}

	// if all of them are erroring I'd want to exit the process,
	return nil
}

// Preparing the payload and sending bulk events to elastic search.
func (h *BatchHandler) sendToES(ctx context.Context, events []*models.SearchDataImport) error {

	log.Info(ctx, "bulk events into ES starts")
	target := len(events)

	fmt.Println(target, events[0].SearchIndex)
	var bulkupsert []byte
	for _, event := range events {
		if event.UID == "" {
			log.Info(ctx, "no uid for inbound kafka event, no transformation possible")
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

	jsonUpsertResponse, err := h.esClient.BulkUpdate(ctx, esDestIndex, h.esURL, bulkupsert)
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
			if resUpsertItem[esRespCreate].Status == 409 {
				continue
			} else {
				log.Error(ctx, "error upserting doc to ES", err,
					log.Data{
						"response.uid:":   resUpsertItem[esRespCreate].ID,
						"response status": resUpsertItem[esRespCreate].Status,
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
func prepareEventForBulkUpsertRequestBody(ctx context.Context, sdModel *models.SearchDataImport) (bulkbody []byte, err error) {

	uid := sdModel.UID
	t := transform.NewTransformer()
	esModel := t.TransformEventModelToEsModel(sdModel)

	if esModel != nil {
		b, err := json.Marshal(esModel)
		if err != nil {
			log.Error(ctx, "error marshal to json while preparing bulk request", err)
			return nil, err
		}

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
