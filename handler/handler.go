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
		fmt.Println(string(msg.GetData()))
		if err := s.Unmarshal(msg.GetData(), e); err != nil {
			fmt.Println("Error on line 59")
			return &Error{
				err: fmt.Errorf("failed to unmarshal event: %w", err),
				logData: map[string]interface{}{
					"msg_data": string(msg.GetData()),
				},
			}
		}

		brlOpt := berlin.OptInit()
		brlOpt.Q("London")

		brlResults, err := h.berlinClient.GetBerlin(ctx, brlOpt)
		if err != nil {
			log.Error(ctx, "There was an error getting berlin results", err)
		}

		fmt.Println("These are the berlin results")
		fmt.Println(brlResults.Matches[0].Loc.Subdivision[0])
		// example how to fill the location
		e.Location = brlResults.Matches[0].Loc.Subdivision[0]
		fmt.Println("keywords", e.Location)
		fmt.Println(e.SearchIndex)

		eventMap[e.SearchIndex] = append(eventMap[e.SearchIndex], e)
		fmt.Println("event looks like: ", eventMap[e.SearchIndex][0].Location)
	}

	log.Info(ctx, "batch of events received", log.Data{"len": len(events)})
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

		fmt.Println("upsertBulkRequestBody starts here")
		upsertBulkRequestBody, err := prepareEventForBulkUpsertRequestBody(ctx, event)
		if err != nil {
			fmt.Println("upsertBulkRequestBody errors")
			log.Error(ctx, "error in preparing the bulk for upsert", err, log.Data{
				"event": *event,
			})
			continue
		}
		fmt.Println("upsertBulkRequestBody appends")
		bulkupsert = append(bulkupsert, upsertBulkRequestBody...)
		fmt.Println(upsertBulkRequestBody)
	}

	fmt.Println("jsonUpsertResponse starts")
	uri := fmt.Sprintf("%s/%s/_bulk", h.esURL, esDestIndex)
	fmt.Println("yo this is the fcking URI: ", uri)

	printableString := string(bulkupsert)
	fmt.Println("this is the bulkupserts", printableString)
	jsonUpsertResponse, err := h.esClient.BulkUpdate(ctx, esDestIndex, h.esURL, bulkupsert)
	if err != nil {
		fmt.Println("jsonUpsertResponse errors")
		if jsonUpsertResponse == nil {
			fmt.Println("this is the err: ", err)
			log.Error(ctx, "server error while upserting the event", err)
			return err
		}
		fmt.Println("warning logged")
		log.Warn(ctx, "error in response from elasticsearch while upserting the event", log.FormatErrors([]error{err}))
	}

	fmt.Println("bulkRes starts here")
	var bulkRes models.EsBulkResponse
	if err := json.Unmarshal(jsonUpsertResponse, &bulkRes); err != nil {
		fmt.Println("bulkRes errors")
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
	fmt.Println("prepareEventForBulkUpsertRequestBody entered")
	uid := sdModel.UID
	fmt.Println("check uid ", uid)
	t := transform.NewTransformer()
	esModel := t.TransformEventModelToEsModel(sdModel)

	if esModel != nil {
		fmt.Println("entering empty es model")
		b, err := json.Marshal(esModel)
		if err != nil {
			fmt.Println("error while marshaling bulk request")
			log.Error(ctx, "error marshal to json while preparing bulk request", err)
			return nil, err
		}

		fmt.Println("bulk body ")
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
